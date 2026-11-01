/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <limits>

#include "compute/VeloxBackend.h"
#include "compute/VeloxRuntime.h"
#include "compute/WholeStageResultIterator.h"
#include "config/VeloxConfig.h"
#include "memory/VeloxColumnarBatch.h"
#include "memory/VeloxMemoryManager.h"
#include "substrait/VeloxToSubstraitType.h"
#include "threads/ThreadManager.h"
#include "velox/common/file/File.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

namespace gluten {

class ParallelExecutionTest : public ::testing::TestWithParam<int32_t>, public test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    VeloxBackend::create(
        AllocationListener::noop(), {{kParallelExecutionEnabled, "true"}, {kNumTaskSlotsPerExecutor, "2"}});
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  static void TearDownTestSuite() {
    VeloxBackend::get()->tearDown();
  }

  void writeParquet(const std::string& path, const RowVectorPtr& rows) {
    auto sink =
        std::make_unique<dwio::common::WriteFileSink>(std::make_unique<LocalWriteFile>(path, false, true), path);
    dwio::common::WriterOptions options;
    options.memoryPool = rootPool_.get();
    parquet::Writer writer(std::move(sink), options, asRowType(rows->type()));
    writer.write(rows);
    writer.close();
  }

  static std::string serializeFilteredScan(const RowTypePtr& type, int32_t filterColumn) {
    google::protobuf::Arena arena;
    ::substrait::Plan plan;
    auto* filter = plan.add_relations()->mutable_rel()->mutable_filter();
    filter->mutable_condition()->mutable_selection()->mutable_direct_reference()->mutable_struct_field()->set_field(
        filterColumn);
    auto* read = filter->mutable_input()->mutable_read();
    read->mutable_common()->mutable_direct();
    read->mutable_base_schema()->CopyFrom(VeloxToSubstraitTypeConvertor().toSubstraitNamedStruct(arena, type));
    return plan.SerializeAsString();
  }

  static std::string serializeParquetFiles(const std::vector<std::string>& paths) {
    ::substrait::ReadRel_LocalFiles files;
    for (const auto& path : paths) {
      auto* file = files.add_items();
      file->set_uri_file(path);
      file->set_length(std::numeric_limits<uint64_t>::max());
      file->mutable_parquet();
    }
    return files.SerializeAsString();
  }

  static std::vector<int64_t> readIds(ResultIterator& iter) {
    std::vector<int64_t> ids;
    while (iter.hasNext()) {
      auto rows = std::dynamic_pointer_cast<VeloxColumnarBatch>(iter.next())->getRowVector();
      DecodedVector decoded(*rows->childAt(0));
      for (vector_size_t row = 0; row < rows->size(); ++row) {
        ids.push_back(decoded.valueAt<int64_t>(row));
      }
    }
    std::sort(ids.begin(), ids.end());
    return ids;
  }

  std::shared_ptr<exec::test::TempDirectoryPath> tempDir_{exec::test::TempDirectoryPath::create()};
};

TEST_P(ParallelExecutionTest, filteredScan) {
  const int32_t taskDrivers = GetParam();
  const int32_t numFiles = 4;
  const int32_t rowsPerFile = 100;
  const auto type = ROW({"id", "keep"}, {BIGINT(), BOOLEAN()});
  std::vector<std::string> paths;
  std::vector<int64_t> expectedIds;
  for (int32_t file = 0; file < numFiles; ++file) {
    paths.push_back(fmt::format("{}/part{}.parquet", tempDir_->getPath(), file));
    writeParquet(
        paths.back(),
        makeRowVector(
            type->names(),
            {makeFlatVector<int64_t>(rowsPerFile, [&](auto row) { return file * rowsPerFile + row; }),
             makeFlatVector<bool>(rowsPerFile, [](auto row) { return row % 2 == 0; })}));
    for (int32_t row = 0; row < rowsPerFile; row += 2) {
      expectedIds.push_back(file * rowsPerFile + row);
    }
  }
  const auto plan = serializeFilteredScan(type, 1);
  const auto files = serializeParquetFiles(paths);

  auto* threadManager = ThreadManager::create(kVeloxBackendKind, ThreadInitializer::noop());
  auto vmm = std::make_unique<VeloxMemoryManager>(
      kVeloxBackendKind, AllocationListener::noop(), *VeloxBackend::get()->getBackendConf());
  {
    VeloxRuntime runtime(
        kVeloxBackendKind,
        vmm.get(),
        threadManager,
        {{kSessionTimezone, "UTC"},
         {kParallelExecutionEnabled, "true"},
         {kParallelExecutionTaskDrivers, std::to_string(taskDrivers)}});
    runtime.parsePlan(reinterpret_cast<const uint8_t*>(plan.data()), plan.size());
    runtime.parseSplitInfo(reinterpret_cast<const uint8_t*>(files.data()), files.size(), 0);
    auto iter = runtime.createResultIterator(tempDir_->getPath() + "/spill");
    runtime.noMoreSplits(iter.get());

    EXPECT_EQ(readIds(*iter), expectedIds);

    auto* wholeStage = dynamic_cast<WholeStageResultIterator*>(iter->getInputIter());
    const auto planStats = exec::toPlanStats(wholeStage->task()->taskStats());
    const auto* veloxPlan = wholeStage->veloxPlan();
    EXPECT_EQ(planStats.at(*veloxPlan->leafPlanNodeIds().begin()).numDrivers, taskDrivers);
    EXPECT_EQ(planStats.at(veloxPlan->id()).numDrivers, 1);
  }
  vmm.reset();
  ThreadManager::release(threadManager);
}

INSTANTIATE_TEST_SUITE_P(TaskDrivers, ParallelExecutionTest, ::testing::Values(1, 4));

} // namespace gluten

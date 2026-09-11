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

#include "compute/VeloxBackendExtension.h"

#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"

namespace gluten {
namespace {

using facebook::velox::config::ConfigBase;

ConfigBase emptyConf() {
  return ConfigBase({});
}

class VeloxBackendExtensionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    testingUnregisterAllVeloxBackendExtensions();
  }

  void TearDown() override {
    testingUnregisterAllVeloxBackendExtensions();
  }
};

TEST_F(VeloxBackendExtensionTest, runsInRegistrationOrder) {
  std::vector<std::string> ran;
  registerVeloxBackendExtension("first", [&](const ConfigBase&) { ran.push_back("first"); });
  registerVeloxBackendExtension("second", [&](const ConfigBase&) { ran.push_back("second"); });

  EXPECT_EQ(registeredVeloxBackendExtensions(), (std::vector<std::string>{"first", "second"}));

  auto conf = emptyConf();
  initVeloxBackendExtensions(conf);

  EXPECT_EQ(ran, (std::vector<std::string>{"first", "second"}));
}

TEST_F(VeloxBackendExtensionTest, receivesBackendConf) {
  ConfigBase conf({{"spark.gluten.test.key", "value"}});
  std::string seen;
  registerVeloxBackendExtension(
      "reader", [&](const ConfigBase& c) { seen = c.get<std::string>("spark.gluten.test.key", ""); });

  initVeloxBackendExtensions(conf);

  EXPECT_EQ(seen, "value");
}

TEST_F(VeloxBackendExtensionTest, rejectsDuplicateName) {
  registerVeloxBackendExtension("dup", [](const ConfigBase&) {});

  VELOX_ASSERT_THROW(
      registerVeloxBackendExtension("dup", [](const ConfigBase&) {}),
      "Velox backend extension 'dup' is already registered.");
}

TEST_F(VeloxBackendExtensionTest, rejectsNullExtension) {
  VELOX_ASSERT_THROW(registerVeloxBackendExtension("null", nullptr), "Velox backend extension 'null' is null.");
}

TEST_F(VeloxBackendExtensionTest, rejectsRegistrationAfterInit) {
  auto conf = emptyConf();
  initVeloxBackendExtensions(conf);

  VELOX_ASSERT_THROW(
      registerVeloxBackendExtension("late", [](const ConfigBase&) {}), "was registered after VeloxBackend::init() ran");
}

TEST_F(VeloxBackendExtensionTest, clearsTheRegistryAfterRunning) {
  int runs = 0;
  registerVeloxBackendExtension("once", [&](const ConfigBase&) { ++runs; });

  auto first = emptyConf();
  initVeloxBackendExtensions(first);
  auto second = emptyConf();
  initVeloxBackendExtensions(second);

  EXPECT_EQ(runs, 1);
  EXPECT_TRUE(registeredVeloxBackendExtensions().empty());
}

TEST_F(VeloxBackendExtensionTest, initWithNoExtensionsIsANoOp) {
  auto conf = emptyConf();
  EXPECT_NO_THROW(initVeloxBackendExtensions(conf));
  EXPECT_TRUE(registeredVeloxBackendExtensions().empty());
}

} // namespace
} // namespace gluten

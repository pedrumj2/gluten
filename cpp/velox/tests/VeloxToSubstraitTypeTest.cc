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

#include "velox/common/base/tests/GTestUtils.h"

#include "substrait/SubstraitParser.h"
#include "substrait/TypeUtils.h"
#include "substrait/VeloxToSubstraitType.h"

using namespace facebook::velox;

namespace gluten {

class VeloxToSubstraitTypeTest : public ::testing::Test {
 protected:
  void testTypeConversion(const TypePtr& type) {
    SCOPED_TRACE(type->toString());

    google::protobuf::Arena arena;
    auto substraitType = typeConvertor_->toSubstraitType(arena, type);
    auto sameType = SubstraitParser::parseType(substraitType);
    ASSERT_TRUE(sameType->kindEquals(type))
        << "Expected: " << type->toString() << ", but got: " << sameType->toString();
  }

  /// Same round trip, but compares with operator== rather than kindEquals so that row
  /// field names are checked too. kindEquals ignores them, which is why the named rows
  /// in the `basic` case above passed while the names were being dropped.
  void testTypeConversionPreservesNames(const TypePtr& type) {
    SCOPED_TRACE(type->toString());

    google::protobuf::Arena arena;
    auto substraitType = typeConvertor_->toSubstraitType(arena, type);
    auto sameType = SubstraitParser::parseType(substraitType);
    ASSERT_TRUE(*sameType == *type) << "Expected: " << type->toString() << ", but got: " << sameType->toString();
  }

  std::shared_ptr<VeloxToSubstraitTypeConvertor> typeConvertor_ = std::make_shared<VeloxToSubstraitTypeConvertor>();
};

TEST_F(VeloxToSubstraitTypeTest, basic) {
  testTypeConversion(BOOLEAN());

  testTypeConversion(TINYINT());
  testTypeConversion(SMALLINT());
  testTypeConversion(INTEGER());
  testTypeConversion(BIGINT());

  testTypeConversion(REAL());
  testTypeConversion(DOUBLE());

  testTypeConversion(VARCHAR());
  testTypeConversion(VARBINARY());

  testTypeConversion(ARRAY(BIGINT()));
  testTypeConversion(MAP(BIGINT(), DOUBLE()));

  testTypeConversion(ROW({"a", "b", "c"}, {BIGINT(), BOOLEAN(), VARCHAR()}));
  testTypeConversion(ROW({"a", "b", "c"}, {BIGINT(), ROW({"x", "y"}, {BOOLEAN(), VARCHAR()}), REAL()}));
  testTypeConversion(ROW({}, {}));
}

// A UDF argument whose ROW loses its field names cannot bind in UDFResolver.tryBindStrict,
// which compares with DataTypeUtils.sameType and so requires the names to match.
TEST_F(VeloxToSubstraitTypeTest, rowFieldNames) {
  testTypeConversionPreservesNames(ROW({"a", "b", "c"}, {BIGINT(), BOOLEAN(), VARCHAR()}));
  testTypeConversionPreservesNames(ROW({"a", "b", "c"}, {BIGINT(), ROW({"x", "y"}, {BOOLEAN(), VARCHAR()}), REAL()}));
}

TEST_F(VeloxToSubstraitTypeTest, rowFieldNamesInContainers) {
  testTypeConversionPreservesNames(ROW({"arr"}, {ARRAY(ROW({"elem"}, {VARCHAR()}))}));
  testTypeConversionPreservesNames(ROW({"m"}, {MAP(VARCHAR(), ROW({"v"}, {DOUBLE()}))}));
}

} // namespace gluten

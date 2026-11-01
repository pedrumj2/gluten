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
package org.apache.gluten.execution.python

import org.apache.gluten.execution.WholeStageTransformerSuite
import org.apache.gluten.expression.UDFMappings

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.python.{DummyUDF, EvalPythonExecTransformer, UserDefinedPythonFunction}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

class EvalPythonExecTransformerSuite extends WholeStageTransformerSuite {

  import testImplicits.localSeqToDatasetHolder
  import testImplicits.newProductEncoder

  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"
  // Spark exposes this value through a package-private PythonEvalType object.
  private val SQL_BATCHED_UDF = 100
  // DummyUDF cannot run in a Python worker, so a query calling this UDF succeeds only when
  // Velox evaluates the native function the UDF is mapped to.
  private val pyUpper = UserDefinedPythonFunction(
    name = "pyupper",
    func = new DummyUDF,
    dataType = StringType,
    pythonEvalType = SQL_BATCHED_UDF,
    udfDeterministic = true)

  override def beforeAll(): Unit = {
    super.beforeAll()
    // The plugin loads UDF mappings only when the first SparkSession of the JVM starts.
    UDFMappings.pythonUDFMap.put("pyupper", "upper")
  }

  override protected def afterAll(): Unit = {
    UDFMappings.pythonUDFMap.remove("pyupper")
    super.afterAll()
  }

  test("offload a python udf mapped to a native function") {
    val df = Seq(("a", 1), ("b", 2), (null, 3))
      .toDF("s", "i")
      .select(col("i"), pyUpper(col("s")))
    checkGlutenPlan[EvalPythonExecTransformer](df)
    checkAnswer(df, Seq(Row(1, "A"), Row(2, "B"), Row(3, null)))
  }
}

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
package org.apache.spark.sql.execution.python

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.{TransformContext, TransformSupport, UnaryTransformSupport}
import org.apache.gluten.execution.ValidationResult
import org.apache.gluten.expression._
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression._
import org.apache.gluten.substrait.rel._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan

import java.util.{ArrayList => JArrayList, List => JList}

import scala.collection.JavaConverters._

case class EvalPythonExecTransformer(
    udfs: Seq[PythonUDF],
    resultAttrs: Seq[Attribute],
    child: SparkPlan)
  extends EvalPythonExecBase
  with UnaryTransformSupport {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genFilterTransformerMetrics(sparkContext)

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genFilterTransformerMetricsUpdater(metrics)

  override protected def withNewChildInternal(newChild: SparkPlan): EvalPythonExecTransformer =
    copy(udfs, resultAttrs, newChild)

  override protected def doValidateInternal(): ValidationResult = {
    // All udfs should be scalar python udf
    for (udf <- udfs) {
      if (!PythonUDF.isScalarPythonUDF(udf)) {
        return ValidationResult.failed(s"$udf is not scalar python udf")
      }
    }

    val context = new SubstraitContext
    val operatorId = context.nextOperatorId(this.nodeName)

    val expressionNodes = new JArrayList[ExpressionNode]
    child.output.zipWithIndex.foreach(
      x => expressionNodes.add(ExpressionBuilder.makeSelection(x._2)))
    udfs.foreach(
      udf => {
        expressionNodes.add(
          ExpressionConverter
            .replaceWithExpressionTransformer(udf, child.output)
            .doTransform(context))
      })

    val relNode = getRelNode(null, expressionNodes, context, operatorId, child.output, true)

    doNativeValidation(context, relNode)
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].transform(context)
    val operatorId = context.nextOperatorId(this.nodeName)
    val expressionNodes = new JArrayList[ExpressionNode]
    child.output.zipWithIndex.foreach(
      x => expressionNodes.add(ExpressionBuilder.makeSelection(x._2)))
    udfs.foreach(
      udf => {
        expressionNodes.add(
          ExpressionConverter
            .replaceWithExpressionTransformer(udf, child.output)
            .doTransform(context))
      })

    val relNode =
      getRelNode(childCtx.root, expressionNodes, context, operatorId, child.output, false)
    TransformContext(output, relNode)
  }

  def getRelNode(
      input: RelNode,
      expressionNodes: JList[ExpressionNode],
      context: SubstraitContext,
      operatorId: Long,
      inputAttributes: Seq[Attribute],
      validation: Boolean): RelNode = {
    RelBuilder.makeProjectRel(
      inputAttributes.asJava,
      input,
      expressionNodes,
      context,
      operatorId,
      validation)
  }
}

/*
 * Copyright (c) 2016 Mashin (http://mashin.io). All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mashin.rich.spark

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.{LinearRegressionModel, LabeledPoint}
import org.apache.spark.rdd.RDD

import scala.util.Random

object GradientDescentDataGen {

  val dataSize: Int = 10000
  val d: Int = 10
  val wOriginal: Vector = Vectors.dense(Array.tabulate(d)(_ => Random.nextGaussian))
  val stepSize: Double = 1e-2
  val numIterations: Int = 10
  val numIterations2: Int = 1000
  val w0: Vector = Vectors.dense(Array.tabulate(d)(_ => 10 * Random.nextDouble))
  val regParam: Double = 1e-6
  val miniBatchFraction: Double = 1
  val convergenceTol: Double = 0

  def f(x: Vector, w: Vector = wOriginal): Double = {
    (0 until d).map(i => x(i) * w(i)).sum
  }

  def generate(implicit sc: SparkContext): RDD[(Double, Vector)] = {
    sc.range(0, dataSize, 1, 4)
      .map(seed => {
        Random.setSeed(seed)
        Vectors.dense(Array.tabulate[Double](d)(_ => Random.nextDouble))
      })
      .map(v => (f(v), v))
  }

  implicit def labeledPointsToPairs(rdd: RDD[LabeledPoint]): RDD[(Double, Vector)] = {
    rdd.map(p => (p.label, p.features))
  }

  implicit def pairsToLabeledPoints(rdd: RDD[(Double, Vector)]): RDD[LabeledPoint] = {
    rdd.map(p => LabeledPoint(p._1, p._2))
  }

  def predictionAndObservations(data: RDD[(Double, Vector)], model: LinearRegressionModel)
    : RDD[(Double, Double)] = {
    model.predict(data.map(_._2)).zip(data.map(_._1))
  }

  def rmse(data: RDD[(Double, Vector)], weights: Vector): Double = {
    val bcWeights = data.context.broadcast(weights)
    val se = data.treeAggregate(0.0)(
      seqOp = (s, point) => Math.pow(f(point._2, bcWeights.value) - point._1, 2),
      combOp = _ + _
    )
    Math.sqrt(se / data.count())
  }

}

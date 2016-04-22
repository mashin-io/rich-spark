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

import io.mashin.rich.spark.GradientDescentDataGen._
import org.apache.spark.mllib.optimization.{ParallelSGD, GradientDescent, LeastSquaresGradient, SquaredL2Updater}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

class GradientDescentSuite extends FunSuite with ShouldMatchers {

  private def sparkContext(name: String): SparkContext = {
    new SparkContext(new SparkConf().setAppName(name).setMaster("local[*]"))
  }

  test("MLLib Gradient Descent") {
    implicit val sc = sparkContext("MLLib-Gradient-Descent")

    val data = generate

    val gradient = new LeastSquaresGradient
    val updater = new SquaredL2Updater

    val (wHat, losses) = GradientDescent.runMiniBatchSGD(
      data, gradient, updater,
      stepSize, numIterations2,
      regParam, miniBatchFraction,
      w0, convergenceTol)

    println("losses: " + losses.toList.mkString(", "))
    println("wOriginal: " + wOriginal)
    println("wHat: " + wHat)
    println(s"RMSE: ${rmse(data, wHat)}")

    sc.stop()
  }

  test("Rich-Spark Parallel Stochastic Gradient Descent") {
    implicit val sc = sparkContext("Rich-Spark-Parallel-Stochastic-Gradient-Descent")

    val data = generate

    val gradient = new LeastSquaresGradient
    val updater = new SquaredL2Updater

    val (wHat, losses) = ParallelSGD.runMiniBatchSGD(
      data, gradient, updater,
      stepSize, numIterations, numIterations2,
      regParam, miniBatchFraction,
      w0, convergenceTol)

    println("losses: " + losses.toList.mkString(", "))
    println("wOriginal: " + wOriginal)
    println("wHat: " + wHat)
    println(s"RMSE: ${rmse(data, wHat)}")

    sc.stop()
  }

}

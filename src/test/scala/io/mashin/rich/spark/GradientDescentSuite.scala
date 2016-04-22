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
import org.apache.spark.mllib.optimization.{GradientDescent, LeastSquaresGradient,
                                            ParallelSGD, SquaredL2Updater}

class GradientDescentSuite extends RichSparkTestSuite {

  sparkTest("MLLib Gradient Descent") {sc =>
    val data = generate(sc)

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
  }

  sparkTest("Rich-Spark Parallel Stochastic Gradient Descent") {sc =>
    val data = generate(sc)

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
  }

}

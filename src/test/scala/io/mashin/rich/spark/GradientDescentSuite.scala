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
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.optimization._

class GradientDescentSuite extends RichSparkTestSuite {

  private def testCase(sc: SparkContext, gradient: Gradient, updater: Updater) {
    val data = generate(sc).cache()
    data.count()

    var res1: (Vector, Array[Double]) = null
    val t1 = time {
      res1 = ParallelSGD.runMiniBatchSGD(
        data, gradient, updater,
        stepSize, numIterations, numIterations2,
        regParam, miniBatchFraction,
        w0, convergenceTol)
    }
    val (wHat1, losses1) = res1
    val rmse1 = rmse(data, wHat1)

    var res2: (Vector, Array[Double]) = null
    val t2 = time {
      res2 = GradientDescent.runMiniBatchSGD(
        data, gradient, updater,
        stepSize, numIterations * numIterations2,
        regParam, miniBatchFraction,
        w0, convergenceTol)
    }
    val (wHat2, losses2) = res2
    val rmse2 = rmse(data, wHat2)

    t1 should be < t2
    rmse1 should be < rmse2
    losses1.last should be < losses2.last

    println("ParallelSGD losses: " + losses1.toList.mkString(", "))
    println("GradientDescent losses: " + losses2.toList.mkString(", "))

    println("wOriginal: " + wOriginal)
    println("ParallelSGD wHat: " + wHat1)
    println("GradientDescent wHat: " + wHat2)

    println(s"ParallelSGD RMSE: ${rmse1}")
    println(s"GradientDescent RMSE: ${rmse2}")

    println(s"ParallelSGD Time: ${formatDuration(t1)}")
    println(s"GradientDescent Time: ${formatDuration(t2)}")

    println(s"ParallelSGD (${formatDuration(t1)}) " +
      s"is ${t2.toDouble/t1.toDouble}X" +
      s" faster than GradientDescent (${formatDuration(t2)})")
    println(s"RMSE: ParallelSGD " +
      s"is ${rmse2/rmse1}X" +
      s" more accurate than GradientDescent")
  }

  sparkTest("ParallelSGD VS GradientDescent") {sc =>
    val gradient = new LeastSquaresGradient
    val updater = new SquaredL2Updater
    testCase(sc, gradient, updater)
  }


}

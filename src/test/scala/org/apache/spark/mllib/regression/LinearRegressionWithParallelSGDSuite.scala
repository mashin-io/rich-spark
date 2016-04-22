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

package org.apache.spark.mllib.regression

import io.mashin.rich.spark.GradientDescentDataGen._
import io.mashin.rich.spark.RichSparkTestSuite
import org.apache.spark.mllib.evaluation.RegressionMetrics

class LinearRegressionWithParallelSGDSuite extends RichSparkTestSuite {

  sparkTest("LinearRegressionWithParallelSGD VS LinearRegressionWithSGD") {sc =>
    val data = generate(sc)

    var model1: LinearRegressionModel = null
    val t1 = time {
      model1 = LinearRegressionWithParallelSGD.train(data, numIterations,
        numIterations2, stepSize, miniBatchFraction, w0)
    }

    var model2: LinearRegressionModel = null
    val t2 = time {
      model2 = LinearRegressionWithSGD.train(data,
        numIterations2, stepSize, miniBatchFraction, w0)
    }

    val metrics1 = new RegressionMetrics(predictionAndObservations(data, model1))
    val metrics2 = new RegressionMetrics(predictionAndObservations(data, model2))

    t1 < t2 should be (true)
    metrics1.meanAbsoluteError < metrics2.meanAbsoluteError should be (true)
    metrics1.rootMeanSquaredError < metrics2.rootMeanSquaredError should be (true)

    println(s"LinearRegressionWithParallelSGD is ${t2.toDouble/t1.toDouble}X" +
      s" faster than LinearRegressionWithSGD")
    println(s"meanAbsoluteError: LinearRegressionWithParallelSGD " +
      s"is ${metrics2.meanAbsoluteError/metrics1.meanAbsoluteError}X" +
      s" more accurate than LinearRegressionWithSGD")
    println(s"rootMeanSquaredError: LinearRegressionWithParallelSGD " +
      s"is ${metrics2.rootMeanSquaredError/metrics1.rootMeanSquaredError}X" +
      s" more accurate than LinearRegressionWithSGD")
  }

}

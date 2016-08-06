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

import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.optimization.{LeastSquaresGradient, ParallelSGD, SimpleUpdater}
import org.apache.spark.rdd.RDD

/**
 * Train a linear regression model with no regularization using Parallel Stochastic Gradient Descent.
 * This solves the least squares regression formulation
 *              f(weights) = 1/n ||A weights-y||^2^
 * (which is the mean squared error).
 * Here the data matrix has n rows, and the input RDD holds the set of rows of A, each with
 * its corresponding right hand side label y.
 * See also the documentation for the precise formulation.
 */
class LinearRegressionWithParallelSGD private[mllib] (
    private var stepSize: Double,
    private var numIterations: Int,
    private var numIterations2: Int,
    private var miniBatchFraction: Double)
  extends GeneralizedLinearAlgorithm[LinearRegressionModel] with Serializable {

  private val gradient = new LeastSquaresGradient()
  private val updater = new SimpleUpdater()
  @Since("0.8.0")
  override val optimizer = new ParallelSGD(gradient, updater)
    .setStepSize(stepSize)
    .setNumIterations(numIterations)
    .setNumIterations2(numIterations2)
    .setMiniBatchFraction(miniBatchFraction)

  /**
   * Construct a LinearRegression object with default parameters: {stepSize: 1.0,
   * numIterations: 10, numIterations2 = 100, miniBatchFraction: 1.0}.
   */
  @Since("0.8.0")
  def this() = this(1.0, 10, 100, 1.0)

  override protected[mllib] def createModel(weights: Vector, intercept: Double) = {
    new LinearRegressionModel(weights, intercept)
  }
}

/**
 * Top-level methods for calling LinearRegression.
 *
 */
object LinearRegressionWithParallelSGD {

  /**
   * Train a Linear Regression model given an RDD of (label, features) pairs. We run a fixed number
   * of two levels of iterations.
   * Each level 1 iteration shuffles the data, runs a mini-batch SGD per partition by running the
   * level 2 iterations, and averages the weights calculated on each partition to be used as a seed
   * for the next level 1 iteration.
   * Each level 2 iteration uses `miniBatchFraction` fraction of the data per partition to calculate
   * a stochastic gradient and update the local weights per partition using the specified step size.
   * The weights used in gradient descent are initialized using the initial weights provided.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   * @param numIterations Number of level 1 iterations (the shuffling iterations).
   * @param numIterations2 Number of level 2 iterations (the mini-batch SGD iterations).
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param miniBatchFraction Fraction of data to be used per level 2 iteration.
   * @param initialWeights Initial set of weights to be used. Array should be equal in size to
   *        the number of features in the data.
   *
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      numIterations2: Int,
      stepSize: Double,
      miniBatchFraction: Double,
      initialWeights: Vector): LinearRegressionModel = {
    new LinearRegressionWithParallelSGD(stepSize, numIterations, numIterations2, miniBatchFraction)
      .run(input, initialWeights)
  }

  /**
   * Train a Linear Regression model given an RDD of (label, features) pairs. We run a fixed number
   * of two levels of iterations.
   * Each level 1 iteration shuffles the data, runs a mini-batch SGD per partition by running the
   * level 2 iterations, and averages the weights calculated on each partition to be used as a seed
   * for the next level 1 iteration.
   * Each level 2 iteration uses `miniBatchFraction` fraction of the data per partition to calculate
   * a stochastic gradient and update the local weights per partition using the specified step size.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   * @param numIterations Number of level 1 iterations (the shuffling iterations).
   * @param numIterations2 Number of level 2 iterations (the mini-batch SGD iterations).
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param miniBatchFraction Fraction of data to be used per level 2 iteration.
   *
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      numIterations2: Int,
      stepSize: Double,
      miniBatchFraction: Double): LinearRegressionModel = {
    new LinearRegressionWithParallelSGD(stepSize, numIterations, numIterations2, miniBatchFraction)
      .run(input)
  }

  /**
   * Train a Linear Regression model given an RDD of (label, features) pairs. We run a fixed number
   * of two levels of iterations.
   * Each level 1 iteration shuffles the data, runs a batch SGD per partition by running the
   * level 2 iterations, and averages the weights calculated on each partition to be used as a seed
   * for the next level 1 iteration.
   * Each level 2 iteration uses all the data per partition to calculate
   * a gradient and update the local weights per partition using the specified step size.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   * @param numIterations Number of level 1 iterations (the shuffling iterations).
   * @param numIterations2 Number of level 2 iterations (the mini-batch SGD iterations).
   * @param stepSize Step size to be used for each iteration of Gradient Descent.
   * @return a LinearRegressionModel which has the weights and offset from training.
   *
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      numIterations2: Int,
      stepSize: Double): LinearRegressionModel = {
    train(input, numIterations, numIterations2, stepSize, 1.0)
  }

  /**
   * Train a Linear Regression model given an RDD of (label, features) pairs. We run a fixed number
   * of two levels of iterations.
   * Each level 1 iteration shuffles the data, runs a batch SGD per partition by running the
   * level 2 iterations, and averages the weights calculated on each partition to be used as a seed
   * for the next level 1 iteration.
   * Each level 2 iteration uses all the data per partition to calculate
   * a gradient and update the local weights per partition using a step size of 1.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   * @param numIterations Number of level 1 iterations (the shuffling iterations).
   * @param numIterations2 Number of level 2 iterations (the mini-batch SGD iterations).
   * @return a LinearRegressionModel which has the weights and offset from training.
   *
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      numIterations2: Int): LinearRegressionModel = {
    train(input, numIterations, numIterations2, 1.0, 1.0)
  }
}
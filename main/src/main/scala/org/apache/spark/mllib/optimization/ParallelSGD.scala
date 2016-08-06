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

package org.apache.spark.mllib.optimization

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

import breeze.linalg.{DenseVector => BDV, Vector => BV, norm}

import org.apache.spark.Partitioner
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.{BernoulliSampler, RandomSampler}

private[spark] class RandomPartitioner(val n: Int) extends Partitioner {
  val random = new Random(System.nanoTime)

  override def numPartitions: Int = n

  override def getPartition(key: Any): Int = random.nextInt(n)
}

class ParallelSGD (private var gradient: Gradient, private var updater: Updater)
  extends Optimizer with Logging {

  private var stepSize: Double = 1.0
  private var numIterations: Int = 10
  private var numIterations2: Int = 100
  private var regParam: Double = 0.0
  private var miniBatchFraction: Double = 1.0
  private var convergenceTol: Double = 0.001

  /**
   * Set the initial step size of SGD for the first step. Default 1.0.
   * In subsequent steps, the step size will decrease with stepSize/sqrt(t)
   */
  def setStepSize(step: Double): this.type = {
    this.stepSize = step
    this
  }

  /**
   * :: Experimental ::
   * Set fraction of data to be used for each SGD iteration. Default 1.0
   */
  @Experimental
  def setMiniBatchFraction(fraction: Double): this.type = {
    this.miniBatchFraction = fraction
    this
  }

  /**
   * Set the number of level 1 iterations for Parallel SGD.
   * The number of data shuffle iterations.
   * Default 10.
   */
  def setNumIterations(iters: Int): this.type = {
    this.numIterations = iters
    this
  }

  /**
   * Set the number of level 2 iterations for Parallel SGD.
   * The number of mini-batch SGD iterations per partition
   * of the shuffled data.
   * Default 100.
   */
  def setNumIterations2(iters: Int): this.type = {
    this.numIterations2 = iters
    this
  }

  /**
   * Set the regularization parameter. Default 0.0.
   */
  def setRegParam(regParam: Double): this.type = {
    this.regParam = regParam
    this
  }

  /**
   * Set the convergence tolerance. Default 0.001
   * convergenceTol is a condition which decides iteration termination
   * (both first and second level iterations).
   * The end of iteration is decided based on below logic.
   *
   *  - If the norm of the new solution vector is >1, the diff of solution vectors
   *    is compared to relative tolerance which means normalizing by the norm of
   *    the new solution vector.
   *  - If the norm of the new solution vector is <=1, the diff of solution vectors
   *    is compared to absolute tolerance which is not normalizing.
   *
   * Must be between 0.0 and 1.0 inclusively.
   */
  def setConvergenceTol(tolerance: Double): this.type = {
    require(0.0 <= tolerance && tolerance <= 1.0)
    this.convergenceTol = tolerance
    this
  }

  /**
   * Set the gradient function (of the loss function of one single data example)
   * to be used for SGD.
   */
  def setGradient(gradient: Gradient): this.type = {
    this.gradient = gradient
    this
  }


  /**
   * Set the updater function to actually perform a gradient step in a given direction.
   * The updater is responsible to perform the update from the regularization term as well,
   * and therefore determines what kind or regularization is used, if any.
   */
  def setUpdater(updater: Updater): this.type = {
    this.updater = updater
    this
  }

  /**
   * :: DeveloperApi ::
   * Runs parallel stochastic gradient descent on the given training data.
   *
   * @param data training data
   * @param initialWeights initial weights
   * @return solution vector
   */
  @DeveloperApi
  def optimize(data: RDD[(Double, Vector)], initialWeights: Vector): Vector = {
    val (weights, _) = ParallelSGD.runMiniBatchSGD(
      data,
      gradient,
      updater,
      stepSize,
      numIterations,
      numIterations2,
      regParam,
      miniBatchFraction,
      initialWeights,
      convergenceTol)
    weights
  }

}

/**
 * :: DeveloperApi ::
 * Top-level method to run parallel stochastic gradient descent.
 */
@DeveloperApi
object ParallelSGD extends Logging {
  /**
   * Run parallel stochastic gradient descent (ParallelSGD) using mini batches per partition.
   * In each level 1 iteration, we shuffle the data, then we run mini-batch SGD on each
   * shuffled partition for a number of iterations (= numIterations2) computing weights per
   * partition, then we aggregate the weights from each partition computing the average that
   * would be used as initial weights for the next level 1 iteration.
   *
   * In each level 2 iteration, we sample a subset (fraction miniBatchFraction) of the data per
   * partition in order to compute a gradient estimate. That gradient estimate is the average
   * of subgradients computed for each data point from the sampled batch. The weights are updated
   * locally per partition using the gradient estimate.
   * Sampling, and averaging the subgradients over this subset is performed locally per partition
   * without any spark map-reduce actions.
   *
   * @param data Input data for SGD. RDD of the set of data examples, each of
   *             the form (label, [feature values]).
   * @param gradient Gradient object (used to compute the gradient of the loss function of
   *                 one single data example)
   * @param updater Updater function to actually perform a gradient step in a given direction.
   * @param stepSize initial step size for the first step
   * @param numIterations number of level 1 iterations (the shuffling iterations)
   * @param numIterations2 number of level 2 iterations (the mini-batch SGD iterations)
   * @param regParam regularization parameter
   * @param miniBatchFraction fraction of the data set per partition that should be used for
   *                          one iteration of SGD. Default value 1.0.
   * @param convergenceTol both levels iterations will end before the given number of iterations
   *                       if the relative difference between the current weight and the previous
   *                       weight is less than this value. In measuring convergence, L2 norm is calculated.
   *                       Default value 0.001. Must be between 0.0 and 1.0 inclusively.
   * @return A tuple containing two elements. The first element is a column matrix containing
   *         weights for every feature, and the second element is an array containing the
   *         average stochastic loss computed for every level 1 iteration as the average loss
   *         per partition.
   */
  def runMiniBatchSGD(
      data: RDD[(Double, Vector)],
      gradient: Gradient,
      updater: Updater,
      stepSize: Double,
      numIterations: Int,
      numIterations2: Int,
      regParam: Double,
      miniBatchFraction: Double,
      initialWeights: Vector,
      convergenceTol: Double)
    : (Vector, Array[Double]) = {

    // convergenceTol should be set with non minibatch settings
    if (miniBatchFraction < 1.0 && convergenceTol > 0.0) {
      logWarning("Testing against a convergenceTol when using miniBatchFraction " +
        "< 1.0 can be unstable because of the stochasticity in sampling.")
    }

    val stochasticLossHistory = new ArrayBuffer[Double](numIterations)

    // Record previous weight and current one to calculate solution vector difference
    var previousWeights: Option[Vector] = None
    var currentWeights: Option[Vector] = None

    val numExamples = data.count()

    // if no data, return initial weights to avoid NaNs
    if (numExamples == 0) {
      logWarning("GradientDescent.runMiniBatchSGD returning initial weights, no data found")
      return (initialWeights, stochasticLossHistory.toArray)
    }

    if (numExamples * miniBatchFraction < 1) {
      logWarning("The miniBatchFraction is too small")
    }

    // Initialize weights as a column vector
    var weights = Vectors.dense(initialWeights.toArray)
    val n = weights.size

    val randomPartitioner = new RandomPartitioner(data.getNumPartitions)

    var converged = false // indicates whether converged based on convergenceTol
    var i = 1
    while (!converged && i <= numIterations) {
      val bcWeights = data.context.broadcast(weights)

      /**
       * For the first 2nd level iteration, the regVal will be initialized as sum
       * of weight squares if it's L2 updater; for L1 updater, the same logic is followed.
       */
      val regVal = updater.compute(weights, Vectors.zeros(weights.size), 0, 1, regParam)._2

      val shuffledData = data.partitionBy(randomPartitioner)

      val (weightsSum, lossSum, weightsCount) = shuffledData.mapPartitions(iter => {
        // run mini-batch SGD per partition
        if (iter.isEmpty) {
          // skip partition if empty
          Iterator.single((BDV.zeros[Double](n), 0.0, 0))
        } else {
          val (localWeights, localLoss) = runLocalMiniBatchSGD(
            iter.toSeq, i, gradient, updater, stepSize,
            numIterations, numIterations2, regParam,
            miniBatchFraction, bcWeights.value, regVal, convergenceTol)
          Iterator.single((localWeights, localLoss, 1))
        }
      })
      .treeReduce((v1, v2) => (v1, v2) match {
        case ((weights1, loss1, count1), (weights2, loss2, count2)) =>
          (weights1 += weights2, loss1 + loss2, count1 + count2)
      })

      if (weightsCount > 0) {
        // report average loss per partition
        stochasticLossHistory.append(lossSum / weightsCount)
        // average weights per partition
        weights = Vectors.fromBreeze(weightsSum / weightsCount.toDouble)

        previousWeights = currentWeights
        currentWeights = Some(weights)
        if (previousWeights.isDefined && currentWeights.isDefined) {
          converged = isConverged(previousWeights.get,
            currentWeights.get, convergenceTol)
        }
      } else {
        logWarning(s"Iteration ($i/$numIterations). The size of sampled batch is zero")
      }
      i += 1
    }

    logInfo("ParallelSGD.runMiniBatchSGD finished. Last 10 average stochastic losses %s".format(
      stochasticLossHistory.takeRight(10).mkString(", ")))

    (weights, stochasticLossHistory.toArray)

  }

  def runLocalMiniBatchSGD(
      data: Seq[(Double, Vector)],
      i: Int,
      gradient: Gradient,
      updater: Updater,
      stepSize: Double,
      numIterations: Int,
      numIterations2: Int,
      regParam: Double,
      miniBatchFraction: Double,
      initialWeights: Vector,
      initialRegVal: Double,
      convergenceTol: Double)
    : (BV[Double], Double) = {

    val n = initialWeights.size
    var lossSum = 0.0
    var miniBatchSize = 0L
    var localPreviousWeights: Option[Vector] = None
    var localCurrentWeights: Option[Vector] = None
    var localWeights = initialWeights
    var localRegVal = initialRegVal
    var localConverged = false // indicates whether converged based on convergenceTol

    implicit val sampler = new BernoulliSampler[(Double, Vector)](miniBatchFraction)

    var j = 1
    while (!localConverged && j <= numIterations2) {
      // sample a mini-batch and compute the accumulated gradient
      val (gradAggregate, lossAggregate, miniBatchSizeAggregate) = data
        .sample(System.nanoTime)
        .aggregate[(BV[Double], Double, Long)](
          (BDV.zeros[Double](n), 0.0, 0L))(
          seqop = (c, v) => (c, v) match {
            case ((grad, loss, size), (label, features)) => {
              val l = gradient.compute(features, label, localWeights, Vectors.fromBreeze(grad))
              (grad, loss + l, size + 1)
            }
          },
          combop = (c1, c2) => (c1, c2) match {
            case ((grad1, loss1, miniBatchSize1), (grad2, loss2, miniBatchSize2)) => {
              (grad1 += grad2, loss1 + loss2, miniBatchSize1 + miniBatchSize2)
            }
          }
        )

      if (miniBatchSizeAggregate > 0) {
        lossSum = lossAggregate
        miniBatchSize = miniBatchSizeAggregate

        // update the local weights based on the average gradient in the mini-batch
        val update = updater.compute(
          localWeights, Vectors.fromBreeze(gradAggregate / miniBatchSize.toDouble),
          stepSize, j, regParam)
        localWeights = update._1
        localRegVal = update._2

        localPreviousWeights = localCurrentWeights
        localCurrentWeights = Some(localWeights)
        if (localPreviousWeights.isDefined && localCurrentWeights.isDefined) {
          localConverged = isConverged(localPreviousWeights.get,
            localCurrentWeights.get, convergenceTol)
        }
      } else {
        // shouldn't get here; sampler ensures at least one sampled element
        // regardless from the miniBatchFraction.
        logWarning(s"Iteration ($i/$numIterations, $j/$numIterations2)." +
          " The size of sampled batch is zero")
      }

      j += 1
    }

    //(the updated weights, the regularized average loss of the last iteration)
    (localWeights.toBreeze, lossSum / miniBatchSize + localRegVal)
  }

  /**
   * Alias of [[runMiniBatchSGD]] with convergenceTol set to default value of 0.001.
   */
  def runMiniBatchSGD(
      data: RDD[(Double, Vector)],
      gradient: Gradient,
      updater: Updater,
      stepSize: Double,
      numIterations: Int,
      numIterations2: Int,
      regParam: Double,
      miniBatchFraction: Double,
      initialWeights: Vector): (Vector, Array[Double]) =
    runMiniBatchSGD(data, gradient, updater, stepSize, numIterations,
      numIterations2, regParam, miniBatchFraction, initialWeights, 0.001)


  private def isConverged(
      previousWeights: Vector,
      currentWeights: Vector,
      convergenceTol: Double): Boolean = {
    // To compare with convergence tolerance.
    val previousBDV = previousWeights.toBreeze.toDenseVector
    val currentBDV = currentWeights.toBreeze.toDenseVector

    // This represents the difference of updated weights in the iteration.
    val solutionVecDiff: Double = norm(previousBDV - currentBDV)

    solutionVecDiff < convergenceTol * Math.max(norm(currentBDV), 1.0)
  }

  private implicit def seqToSampledSeq[T: ClassTag](seq: Seq[T])
      (implicit sampler: RandomSampler[T, T]): SampledSeq[T] = {
    new SampledSeq[T](seq, sampler)
  }

}

private class SampledSeq[T: ClassTag](seq: Seq[T], sampler: RandomSampler[T, T]) {
  def sample: Iterator[T] = {
    val iter = sampler.sample(seq.iterator)
    if (iter.isEmpty) {
      Iterator.single(seq(Random.nextInt(seq.size)))
    } else {
      iter
    }
  }

  def sample(seed: Long): Iterator[T] = {
    sampler.setSeed(seed)
    sample
  }
}

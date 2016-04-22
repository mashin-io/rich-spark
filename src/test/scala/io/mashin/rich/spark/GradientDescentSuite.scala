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

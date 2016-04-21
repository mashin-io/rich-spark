package io.mashin.rich.spark

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD

import scala.util.Random

object GradientDescentDataGen {

  val dataSize: Int = 10000
  val d: Int = 10
  val wOriginal: Vector = Vectors.dense(Array.tabulate(d)(_ => Random.nextGaussian))
  val stepSize: Double = 1e-2
  val numIterations: Int = 1000
  val w0: Vector = Vectors.dense(Array.tabulate(d)(_ => 10 * Random.nextDouble))
  val regParam: Double = 1e-6
  val miniBatchFraction: Double = 0.001
  val convergenceTol: Double = 0.0001

  def f(x: Vector): Double = {
    (0 until d).map(i => x(i) * wOriginal(i)).sum
  }

  def generate(implicit sc: SparkContext): RDD[(Double, Vector)] = {
    sc.range(0, dataSize, 1, 4)
      .map(seed => {
        Random.setSeed(seed)
        Vectors.dense(Array.tabulate[Double](d)(_ => Random.nextDouble))
      })
      .map(v => (f(v), v))
  }

}

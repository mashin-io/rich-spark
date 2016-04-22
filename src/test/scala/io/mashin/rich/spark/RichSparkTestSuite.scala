package io.mashin.rich.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, ShouldMatchers}

abstract class RichSparkTestSuite extends FunSuite with ShouldMatchers {

  protected def sparkContext(name: String): SparkContext = {
    new SparkContext(new SparkConf().setAppName(name).setMaster("local[*]"))
  }

  def sparkTest(testCaseName: String)(testProcedure: SparkContext => Unit): Unit = {
    test(testCaseName) {
      implicit val sc = sparkContext(testCaseName)
      testProcedure(sc)
      sc.stop()
    }
  }

  def time(proc: => Unit): Long = {
    val t0 = System.nanoTime
    proc
    System.nanoTime - t0
  }

  def formatDuration(nanos: Long): String = {
    var rem = nanos
    val h = rem / (1000000000L * 60L * 60L)
    rem -= h * (1000000000L * 60L * 60L)
    val m = rem / (1000000000L * 60L)
    rem -= m * (1000000000L * 60L)
    val s = rem / 1000000000L
    rem -= s * 1000000000L
    val ms = rem / 1000000L
    rem -= ms * 1000000L
    val ns = rem

    s"${h}h ${m}m ${s}s ${ms}ms ${ns}ns"
  }

}

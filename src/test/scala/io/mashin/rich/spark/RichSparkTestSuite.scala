package io.mashin.rich.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{ShouldMatchers, FunSuite}

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

}

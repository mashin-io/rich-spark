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

import io.mashin.rich.spark.RichRDD._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{ShouldMatchers, FunSuite}

import scala.util.Random

class RichRDDFunctionsSuite extends FunSuite with ShouldMatchers {

  test("Scan Left RDD") {
    val sc = sparkContext("Scan-Left-RDD")

    val parts = 4
    val partSize = 1000

    val scanLeftTest: (Array[Int], Int, (Int, Int) => Int) => Unit = {(input, init, f) =>
      val rdd = sc.makeRDD(input, parts)
      val rddScanned = rdd.scanLeft(0, init, f)
      rddScanned.collect() should be (input.scanLeft(init)(f))
    }

    val f = (a: Int, b: Int) => a + b
    (-10 to 10).foreach {i =>
      scanLeftTest((1 to parts * partSize).map(_ => i + Random.nextInt(10)).toArray, i, f)
    }

    sc.stop()
  }

  test("Scan Right RDD") {
    val sc = sparkContext("Scan-Right-RDD")

    val parts = 4
    val partSize = 1000

    val scanRightTest: (Array[Int], Int, (Int, Int) => Int) => Unit = {(input, init, f) =>
      val rdd = sc.makeRDD(input, parts)
      val rddScanned = rdd.scanRight(0, init, f)
      rddScanned.collect() should be (input.scanRight(init)(f))
    }

    val f = (a: Int, b: Int) => a + b
    (-10 to 10).foreach {i =>
      scanRightTest((1 to parts * partSize).map(_ => i + Random.nextInt(10)).toArray, i, f)
    }

    sc.stop()
  }

  private def sparkContext(name: String): SparkContext = {
    new SparkContext(new SparkConf().setAppName(name).setMaster("local[*]"))
  }

}

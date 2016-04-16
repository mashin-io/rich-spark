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

class RichPairRDDFunctionsSuite extends FunSuite with ShouldMatchers {

  test("Scan Left Pair RDD") {
    val sc = sparkContext("Scan-Left-Pair-RDD")

    val parts = 4
    val partSize = 2

    val scanLeftTest: (Array[(Int, Int)], Int, (Int, Int) => Int) => Unit = {(input, init, f) =>
      val rdd = sc.makeRDD(input, parts)
      val rddScanned = rdd.scanLeft(0, 0, init, f)
      rddScanned.collect() should be ((0 to input.length).zip(input.map(_._2).scanLeft(init)(f)))
    }

    val f = (a: Int, b: Int) => a + b
    (-10 to 10).foreach {i =>
      scanLeftTest((1 to parts * partSize).zip(
        (1 to parts * partSize).map(_ => i + Random.nextInt(10))).toArray, i, f)
    }

    sc.stop()
  }

  private def sparkContext(name: String): SparkContext = {
    new SparkContext(new SparkConf().setAppName(name).setMaster("local[*]"))
  }

}

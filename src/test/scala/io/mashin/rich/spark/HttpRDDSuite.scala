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

import java.io.{BufferedReader, InputStreamReader}

import io.mashin.rich.spark.RichRDD._
import org.apache.http.client.methods.HttpGet
import org.apache.http.{HttpRequest, HttpResponse}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

class HttpRDDSuite extends FunSuite with ShouldMatchers {

  test("HttpRDD Get") {
    val serverIP = HttpMockConfig.serverIP
    val serverPort = HttpMockConfig.serverPort
    val mockServer = new HttpMock
    mockServer.start()

    val sc = sparkContext("HttpRDD-Get")

    val req: Int => HttpRequest = {i =>
      val pageIndex = i + 1
      new HttpGet(s"http://$serverIP:$serverPort/rdd?page=$pageIndex")
    }

    val res: (Int, HttpResponse) => Iterator[String] = {(i, httpResponse) =>
      val is = new BufferedReader(new InputStreamReader(httpResponse.getEntity.getContent))
      val s = is.readLine()
      is.close()
      s.split(",").iterator
    }

    val numPages = 4
    val rdd = sc.httpRDD(req, res, numPages).cache

    rdd.getNumPartitions should be (numPages)
    rdd.count should be (numPages * HttpMockConfig.perPage)

    rdd.mapPartitionsWithIndex((i, iter) =>
        iter.zipWithIndex.map(t => HttpMockConfig.isValidElement(t._1, i, t._2)))
      .reduce(_ && _) should be (true)

    sc.stop()
    mockServer.stop()
  }

  private def sparkContext(name: String): SparkContext = {
    new SparkContext(new SparkConf().setAppName(name).setMaster("local[*]"))
  }

}

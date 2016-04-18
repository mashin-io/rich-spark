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
import java.util.concurrent.TimeUnit.SECONDS

import io.mashin.rich.spark.RichRDD._
import org.apache.http.client.methods.HttpGet
import org.apache.http.{HttpRequest, HttpResponse}
import org.apache.spark.{SparkConf, SparkContext}
import org.mockserver.client.server.MockServerClient
import org.mockserver.integration.ClientAndServer
import org.mockserver.integration.ClientAndServer.startClientAndServer
import org.mockserver.mock.action.ExpectationCallback
import org.mockserver.model
import org.mockserver.model.HttpCallback.callback
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response
import org.mockserver.model.{Delay, Header}
import org.scalatest._

import scala.collection.JavaConversions

private class TestCallback extends ExpectationCallback {
  override def handle(httpRequest: model.HttpRequest): model.HttpResponse = {
    val pageIndex = JavaConversions.asScalaBuffer(httpRequest.getQueryStringParameters)
      .filter(p => "page".equalsIgnoreCase(p.getName.getValue))
      .head.getValues.get(0).getValue.toInt

    response()
      .withStatusCode(200)
      .withHeaders(
        new Header("Content-Type", "application/text; charset=utf-8")
      )
      .withBody((1 to Config.perPage).map(Config.element(pageIndex, _)).mkString(","))
      .withDelay(new Delay(SECONDS, 1))
  }
}

private object Config {
  var perPage: Int = 1000
  var serverIP = "127.0.0.1"
  var serverPort = 1080

  def element(pageIndex: Int, index: Int): String = s"element-$pageIndex-$index"

  def isValidElement(e: String, partIndex: Int, index: Int): Boolean = {
    element(partIndex + 1, index + 1).equals(e)
  }
}

class HttpRDDSuite extends FunSuite with ShouldMatchers {

  private def mock() {
    new MockServerClient(Config.serverIP, Config.serverPort)
      .when(
        request()
          .withMethod("GET")
          .withPath("/rdd")
      )
      .callback(
        callback()
          .withCallbackClass("io.mashin.rich.spark.TestCallback")
      )
  }

  test("HttpRDD Get") {
    val serverIP = Config.serverIP
    val serverPort = Config.serverPort
    val mockServer: ClientAndServer = startClientAndServer(serverPort)
    mock()

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
    rdd.count should be (numPages * Config.perPage)

    rdd.mapPartitionsWithIndex((i, iter) =>
        iter.zipWithIndex.map(t => Config.isValidElement(t._1, i, t._2)))
      .reduce((a, b) => a && b) should be (true)

    sc.stop()
    mockServer.stop()
  }

  private def sparkContext(name: String): SparkContext = {
    new SparkContext(new SparkConf().setAppName(name).setMaster("local[*]"))
  }

}

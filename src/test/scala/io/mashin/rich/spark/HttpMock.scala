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

import java.util.concurrent.TimeUnit._

import org.mockserver.client.server.MockServerClient
import org.mockserver.integration.ClientAndServer
import org.mockserver.integration.ClientAndServer._
import org.mockserver.mock.action.ExpectationCallback
import org.mockserver.model
import org.mockserver.model.HttpCallback._
import org.mockserver.model.HttpRequest._
import org.mockserver.model.{Delay, Header}
import org.mockserver.model.HttpResponse._

import scala.collection.JavaConversions

class HttpMockCallback extends ExpectationCallback {
  override def handle(httpRequest: model.HttpRequest): model.HttpResponse = {
    val pageIndex = JavaConversions.asScalaBuffer(httpRequest.getQueryStringParameters)
      .filter(p => "page".equalsIgnoreCase(p.getName.getValue))
      .head.getValues.get(0).getValue.toInt

    response()
      .withStatusCode(200)
      .withHeaders(
        new Header("Content-Type", "application/text; charset=utf-8")
      )
      .withBody((1 to HttpMockConfig.perPage).map(HttpMockConfig.element(pageIndex, _)).mkString(","))
      .withDelay(new Delay(SECONDS, 1))
  }
}

object HttpMockConfig {
  var perPage: Int = 1000
  var serverIP = "127.0.0.1"
  var serverPort = 1080

  def element(pageIndex: Int, index: Int): String = s"element-$pageIndex-$index"

  def isValidElement(e: String, partIndex: Int, index: Int): Boolean = {
    element(partIndex + 1, index + 1).equals(e)
  }
}

class HttpMock {
  var mockServer: ClientAndServer = null

  def start() {
    mockServer = startClientAndServer(HttpMockConfig.serverPort)
    new MockServerClient(HttpMockConfig.serverIP, HttpMockConfig.serverPort)
      .when(
        request()
          .withMethod("GET")
          .withPath("/rdd")
      )
      .callback(
        callback()
          .withCallbackClass("io.mashin.rich.spark.HttpMockCallback")
      )
  }

  def stop() {
    if (mockServer != null)
      mockServer.stop()
  }
}
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

package org.apache.spark.streaming.event

import java.net.{HttpURLConnection, URL}
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.concurrent.Eventually._
import spark.utils.IOUtils

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming.{Duration, StreamingContext, TestSuiteBase}

class RESTEventSourceSuite extends TestSuiteBase {

  val host = "localhost"
  val port = 4140
  val amountParam = "amount"
  val pathCounterAdd = "/counter/add"
  val pathCounterSub = "/counter/sub"

  def setupContext(appName: String, batchDuration: Duration): StreamingContext = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(appName)
    new StreamingContext(conf, batchDuration)
  }

  test("REST event source") {
    withStreamingContext(setupContext("REST event source", Duration(100))) { ssc =>

      val counter = new AtomicInteger(0)

      val restServer = ssc.restServer(host, port, "restServer")
        .get(pathCounterAdd).get(pathCounterSub)

      val addJobTrigger = restServer.filter { case event: RESTEvent =>
        event.request.pathInfo.equals(pathCounterAdd)
      }

      val subJobTrigger = restServer.filter { case event: RESTEvent =>
        event.request.pathInfo.equals(pathCounterSub)
      }

      val emptyStream = new ConstantInputDStream(ssc, ssc.sc.emptyRDD[String])

      emptyStream.foreachRDD { (rdd: RDD[String], event: Event) =>
        event match {
          case FilteredEvent(_, RESTEvent(_, request: RESTRequest, _, _)) =>
            assert(request.queryMap.contains(amountParam),
              s"Request missing $amountParam parameter")

            val amount = request.queryMap(amountParam)(0).toInt
            counter.addAndGet(amount)

          case _ =>
            fail(s"Did not expect event $event")
        }
        ()
      }
      .bind(addJobTrigger)

      emptyStream.foreachRDD { (rdd: RDD[String], event: Event) =>
        event match {
          case FilteredEvent(_, RESTEvent(_, request: RESTRequest, _, _)) =>
            assert(request.queryMap.contains(amountParam),
              s"Request missing $amountParam parameter")

            val amount = request.queryMap(amountParam)(0).toInt
            counter.addAndGet(-amount)

          case _ =>
            fail(s"Did not expect event $event")
        }
        ()
      }
      .bind(subJobTrigger)

      ssc.start()

      // wait enough time for the rest server to be started
      Thread.sleep(1000)

      (1 to 10).foreach { i =>
        val (status, _) = request("GET", s"$pathCounterAdd?$amountParam=$i")
        assert(status == 200)
        Thread.sleep(100)
      }

      eventually(eventuallyTimeout) {
        assert(counter.get == (1 to 10).sum)
      }

      (1 to 10).foreach { i =>
        val (status, _) = request("GET", s"$pathCounterSub?$amountParam=$i")
        assert(status == 200)
        Thread.sleep(100)
      }

      eventually(eventuallyTimeout) {
        assert(counter.get == 0)
      }

    }
  }

  def request(method: String, path: String): (Int, String) = {
    val url = new URL(s"http://$host:$port" + path)
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod(method)
    connection.setDoOutput(true)
    connection.connect()
    val body = IOUtils.toString(connection.getInputStream)
    connection.disconnect()
    (connection.getResponseCode, body)
  }

}

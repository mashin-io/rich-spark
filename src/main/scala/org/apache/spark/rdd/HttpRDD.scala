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

package org.apache.spark.rdd

import org.apache.http.client.methods.HttpUriRequest
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.{HttpRequest, HttpResponse}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark._

import scala.reflect.ClassTag

private[spark] class HttpPartition(val idx: Int) extends Partition {
  override def index: Int = idx
}

class HttpRDD[T: ClassTag](
    @transient sc: SparkContext,
    httpRequestFactory: Int => HttpRequest,
    httpResponseHandlerFactory: (Int, HttpResponse) => Iterator[T],
    numPartitions: Int)
  extends RDD[T](sc, Nil) with Logging {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val httpClient = HttpClientBuilder.create().build()
    val httpRequest = httpRequestFactory(split.index)
    val httpResponse = httpRequest match {
      case request: HttpUriRequest => httpClient.execute(request)
      case _ => throw new SparkException(s"not supported http request $httpRequest")
    }
    val iter = httpResponseHandlerFactory(split.index, httpResponse)
    httpResponse.close()
    httpClient.close()
    iter
  }

  override protected def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](numPartitions)(new HttpPartition(_))
  }
}

object HttpRDD {
  def create[T: ClassTag](
      sc: SparkContext,
      httpRequestFactory: Int => HttpRequest,
      httpResponseHandlerFactory: (Int, HttpResponse) => Iterator[T],
      numPartitions: Int)
    : HttpRDD[T] = {
    new HttpRDD[T](sc, httpRequestFactory, httpResponseHandlerFactory, numPartitions)
  }
}

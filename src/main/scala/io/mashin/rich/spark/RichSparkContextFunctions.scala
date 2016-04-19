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

import org.apache.http.{HttpRequest, HttpResponse}
import org.apache.spark.SparkContext
import org.apache.spark.api.java.function.{Function, Function2}
import org.apache.spark.rdd.{HttpRDD, RDD}

import scala.reflect.ClassTag

class RichSparkContextFunctions(sc: SparkContext) {
  /**
   * Creates an RDD based on the responses of HTTP requests equals to the
   * given number of partitions.
   *
   * @param httpRequestFactory constructs an HTTP request given a partition index
   * @param httpResponseHandlerFactory constructs an iterator for the elements of
   *                                   the partition given the partition index and
   *                                   the corresponding HTTP response
   * @param numPartitions the number of partitions
   */
  def httpRDD[T: ClassTag](
      httpRequestFactory: Function[Int, HttpRequest],
      httpResponseHandlerFactory: Function2[Int, HttpResponse, Iterator[T]],
      numPartitions: Int)
    : RDD[T] = {
    HttpRDD.create(sc, httpRequestFactory, httpResponseHandlerFactory, numPartitions)
  }
}

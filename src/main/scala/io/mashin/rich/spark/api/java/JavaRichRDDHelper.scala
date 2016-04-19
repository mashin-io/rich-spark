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

package io.mashin.rich.spark.api.java

import java.util

import io.mashin.rich.spark.RichRDD._
import org.apache.http.{HttpRequest, HttpResponse}
import org.apache.spark.api.java.function.{Function, Function2}
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}

import scala.collection.JavaConversions
import scala.reflect.ClassTag

object JavaRichRDDHelper {

  def httpRDD[T: ClassTag](
      sc: JavaSparkContext,
      httpRequestFactory: Function[Integer, HttpRequest],
      httpResponseHandlerFactory: Function2[Integer, HttpResponse, util.Iterator[T]],
      numPartitions: Int)
    : JavaRDD[T] = {
    val f1: Function[Int, HttpRequest] = (i: Int) => httpRequestFactory.call(i)
    val f2: Function2[Int, HttpResponse, Iterator[T]] =
      (i: Int, res: HttpResponse) => JavaConversions
        .asScalaIterator(httpResponseHandlerFactory.call(i, res))
    sc.sc.httpRDD[T](f1, f2, numPartitions)
  }

  def scanLeft[T: ClassTag](
      rdd: JavaRDD[T],
      zero: T,
      init: T,
      f: Function2[T, T, T])
    : JavaRDD[T] = {
    rdd.rdd.scanLeft(zero, init, f)
  }

  def scanLeft[K: ClassTag, V: ClassTag](
      rdd: JavaPairRDD[K, V],
      zero: V,
      initK: K,
      initV: V,
      f: Function2[V, V, V])
    : JavaPairRDD[K, V] = {
    JavaPairRDD.fromRDD(rdd.rdd.scanLeft(zero, initK, initV, f))
  }

  def scanRight[T: ClassTag](
      rdd: JavaRDD[T],
      zero: T,
      init: T,
      f: Function2[T, T, T])
    : JavaRDD[T] = {
    rdd.rdd.scanRight(zero, init, f)
  }

  def scanRight[K: ClassTag, V: ClassTag](
      rdd: JavaPairRDD[K, V],
      zero: V,
      initK: K,
      initV: V,
      f: Function2[V, V, V])
    : JavaPairRDD[K, V] = {
    JavaPairRDD.fromRDD(rdd.rdd.scanRight(zero, initK, initV, f))
  }

}

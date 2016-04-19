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

package io.mashin.rich.spark;

import io.mashin.rich.spark.api.java.JavaRichRDDHelper;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.Iterator;

public class JavaRichRDD {

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
  public static <T> JavaRDD<T> httpRDD(
      JavaSparkContext sc,
      Function<Integer, HttpRequest> httpRequestFactory,
      Function2<Integer, HttpResponse, Iterator<T>> httpResponseHandlerFactory,
      int numPartitions) {
    return JavaRichRDDHelper.httpRDD(
        sc,
        httpRequestFactory,
        httpResponseHandlerFactory,
        numPartitions,
        RichRDD.<T>fakeClassTag());
  }

  /**
   * Produces an RDD containing cumulative results of applying a
   *  function (binary operator) going left to right.
   *
   *  @param zero  the zero/neutral value s.t. f(zero, other) = other and f(other, zero) = other
   *  @param init  the initial value
   *  @param f     the binary operator applied to the intermediate result and the element
   *  @return      RDD with intermediate results
   */
  public static <T> JavaRDD<T> scanLeft(JavaRDD<T> rdd, T zero, T init, Function2<T, T, T> f) {
    return JavaRichRDDHelper.scanLeft(rdd, zero, init, f, RichRDD.<T>fakeClassTag());
  }

  /**
   * Produces a pair RDD containing cumulative results of applying a
   *  function (binary operator) going left to right.
   *
   *  @param zero   the zero/neutral value s.t. f(zero, other) = other and f(other, zero) = other
   *  @param initK  the key of the initial value
   *  @param initV  the initial value
   *  @param f      the binary operator applied to the intermediate result and the element
   *  @return       RDD with intermediate results
   */
  public static <K, V> JavaPairRDD<K, V> scanLeft(
      JavaPairRDD<K, V> rdd, V zero, K initK, V initV, Function2<V, V, V> f) {
    return JavaRichRDDHelper.scanLeft(
        rdd, zero, initK, initV, f, RichRDD.<K>fakeClassTag(), RichRDD.<V>fakeClassTag());
  }

  /**
   * Produces an RDD containing cumulative results of applying a
   *  function (binary operator) going right to left.
   *  The head of the collection is the last cumulative result.
   *
   *  @param zero  the zero/neutral value s.t. f(zero, other) = other and f(other, zero) = other
   *  @param init  the initial value
   *  @param f     the binary operator applied to the intermediate result and the element
   *  @return      RDD with intermediate results
   */
  public static <T> JavaRDD<T> scanRight(JavaRDD<T> rdd, T zero, T init, Function2<T, T, T> f) {
    return JavaRichRDDHelper.scanRight(rdd, zero, init, f, RichRDD.<T>fakeClassTag());
  }

  /**
   * Produces a pair RDD containing cumulative results of applying a
   *  function (binary operator) going right to left.
   *
   *  @param zero   the zero/neutral value s.t. f(zero, other) = other and f(other, zero) = other
   *  @param initK  the key of the initial value
   *  @param initV  the initial value
   *  @param f      the binary operator applied to the intermediate result and the element
   *  @return       RDD with intermediate results
   */
  public static <K, V> JavaPairRDD<K, V> scanRight(
      JavaPairRDD<K, V> rdd, V zero, K initK, V initV, Function2<V, V, V> f) {
    return JavaRichRDDHelper.scanRight(
        rdd, zero, initK, initV, f, RichRDD.<K>fakeClassTag(), RichRDD.<V>fakeClassTag());
  }

}

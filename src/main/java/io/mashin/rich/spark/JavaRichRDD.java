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

  public static <T> JavaRDD<T> scanLeft(JavaRDD<T> rdd, T zero, T init, Function2<T, T, T> f) {
    return JavaRichRDDHelper.scanLeft(rdd, zero, init, f, RichRDD.<T>fakeClassTag());
  }

  public static <K, V> JavaPairRDD<K, V> scanLeft(
      JavaPairRDD<K, V> rdd, V zero, K initK, V initV, Function2<V, V, V> f) {
    return JavaRichRDDHelper.scanLeft(
        rdd, zero, initK, initV, f, RichRDD.<K>fakeClassTag(), RichRDD.<V>fakeClassTag());
  }

  public static <T> JavaRDD<T> scanRight(JavaRDD<T> rdd, T zero, T init, Function2<T, T, T> f) {
    return JavaRichRDDHelper.scanRight(rdd, zero, init, f, RichRDD.<T>fakeClassTag());
  }

  public static <K, V> JavaPairRDD<K, V> scanRight(
      JavaPairRDD<K, V> rdd, V zero, K initK, V initV, Function2<V, V, V> f) {
    return JavaRichRDDHelper.scanRight(
        rdd, zero, initK, initV, f, RichRDD.<K>fakeClassTag(), RichRDD.<V>fakeClassTag());
  }

}

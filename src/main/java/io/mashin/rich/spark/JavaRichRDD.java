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

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

public class JavaRichRDD {

  public static <T> JavaRDD<T> scanLeft(JavaRDD<T> rdd, T zero, T init, Function2<T, T, T> f) {
    return rdd(rich(rdd).scanLeft(zero, init, toF2(f)));
  }

  public static <K, V> JavaPairRDD<K, V> scanLeft(
      JavaPairRDD<K, V> rdd, V zero, K initK, V initV, Function2<V, V, V> f) {
    return pairRDD(rich(rdd).scanLeft(zero, initK, initV, toF2(f)));
  }

  public static <T> JavaRDD<T> scanRight(JavaRDD<T> rdd, T zero, T init, Function2<T, T, T> f) {
    return rdd(rich(rdd).scanRight(zero, init, toF2(f)));
  }

  public static <K, V> JavaPairRDD<K, V> scanRight(
      JavaPairRDD<K, V> rdd, V zero, K initK, V initV, Function2<V, V, V> f) {
    return pairRDD(rich(rdd).scanRight(zero, initK, initV, toF2(f)));
  }

  private static <T> JavaRDD<T> rdd(RDD<T> rdd) {
    return new JavaRDD<T>(rdd, rdd.elementClassTag());
  }

  private static <K, V> JavaPairRDD<K, V> pairRDD(RDD<Tuple2<K, V>> rdd) {
    return JavaPairRDD.fromJavaRDD(rdd(rdd));
  }

  private static <T> RichRDDFunctions<T> rich(JavaRDD<T> rdd) {
    return RichRDD.rddToRichRDDFunctions(rdd.rdd(), rdd.rdd().elementClassTag());
  }

  private static <K, V> RichPairRDDFunctions<K, V> rich(JavaPairRDD<K, V> rdd) {
    return RichRDD.pairRDDToRichPairRDDFunctions(rdd.rdd(), rdd.kClassTag(), rdd.vClassTag());
  }

  private static <T, U, R> scala.Function2<T, U, R> toF2(final Function2<T, U, R> f) {
    return RichRDD.toScalaFunction2(f);
  }

}

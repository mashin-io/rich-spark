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

import org.apache.spark.rdd.{ScanRDD, RDD}

import scala.reflect.ClassTag

class RichPairRDDFunctions[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) extends Serializable {

  /**
   * Produces a pair RDD containing cumulative results of applying a
   *  function (binary operator) going left to right.
   *
   *  @param zero   the zero/neutral value s.t. f(zero, other) = other and f(other, zero) = other
   *  @param initK  the key of the initial value
   *  @param initV  the initial value
   *  @param f      the binary operator applied to the intermediate result and the element
   *  @return       RDD with intermediate results
   *  @example      {{{
   *    RDD((1, 1), (2, 2), (3, 3), (4, 4)).scanLeft(0, 0, 1, _ + _)
   *     == RDD((0, 1), (1, 2), (2, 4), (3, 7), (4, 11))
   *  }}}
   */
  def scanLeft(zero: V, initK: K, initV: V, f: (V, V) => V): RDD[(K, V)] = {
    val pairF: ((K, V), (K, V)) => (K, V) = {
      case ((scanKey: K, scanValue: V), (key: K, value: V)) => (key, f(scanValue, value))
    }
    ScanRDD.scanLeft[(K, V)](rdd, (initK, zero), (initK, initV), pairF)
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
   *  @example      {{{
   *    RDD((1, 1), (2, 2), (3, 3), (4, 4)).scanRight(0, 5, 1, _ + _)
   *     == RDD((1, 11), (2, 10), (3, 8), (4, 5), (5, 1))
   *  }}}
   */
  def scanRight(zero: V, initK: K, initV: V, f: (V, V) => V): RDD[(K, V)] = {
    val pairF: ((K, V), (K, V)) => (K, V) = {
      case ((key: K, value: V), (scanKey: K, scanValue: V)) => (key, f(value, scanValue))
    }
    ScanRDD.scanRight[(K, V)](rdd, (initK, zero), (initK, initV), pairF)
  }

}
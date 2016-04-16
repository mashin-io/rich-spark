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

class RichRDDFunctions[T: ClassTag](rdd: RDD[T]) extends Serializable {

  /**
   * Produces an RDD containing cumulative results of applying a
   *  function (binary operator) going left to right.
   *
   *  @param zero  the zero/neutral value s.t. f(zero, other) = other and f(other, zero) = other
   *  @param init  the initial value
   *  @param f     the binary operator applied to the intermediate result and the element
   *  @return      RDD with intermediate results
   *  @example     {{{
   *    RDD(1, 2, 3, 4).scanLeft(0, 1, _ + _) == RDD(1, 2, 4, 7, 11)
   *  }}}
   */
  def scanLeft(zero: T, init: T, f: (T, T) => T): RDD[T] = {
    ScanRDD.scanLeft[T](rdd, zero, init, f)
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
   *  @example     {{{
   *    RDD(1, 2, 3, 4).scanRight(0, 1, _ + _) == RDD(11, 10, 8, 5, 1)
   *  }}}
   */
  def scanRight(zero: T, init: T, f: (T, T) => T): RDD[T] = {
    ScanRDD.scanRight[T](rdd, zero, init, f)
  }

}
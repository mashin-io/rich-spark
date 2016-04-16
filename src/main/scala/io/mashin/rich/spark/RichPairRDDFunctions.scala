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

  def scanLeft(zero: V, initK: K, initV: V, f: (V, V) => V): RDD[(K, V)] = {
    val pairF: ((K, V), (K, V)) => (K, V) = {
      case ((scanKey: K, scanValue: V), (key: K, value: V)) => (key, f(scanValue, value))
    }
    ScanRDD.scanLeft[(K, V)](rdd, (initK, zero), (initK, initV), pairF)
  }

  def scanRight(zero: V, initK: K, initV: V, f: (V, V) => V): RDD[(K, V)] = {
    val pairF: ((K, V), (K, V)) => (K, V) = {
      case ((key: K, value: V), (scanKey: K, scanValue: V)) => (key, f(value, scanValue))
    }
    ScanRDD.scanRight[(K, V)](rdd, (initK, zero), (initK, initV), pairF)
  }

}
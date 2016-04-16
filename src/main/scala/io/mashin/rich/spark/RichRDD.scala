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

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object RichRDD {
  implicit def rddToRichRDDFunctions[T: ClassTag](rdd: RDD[T]): RichRDDFunctions[T] =
    new RichRDDFunctions[T](rdd)

  implicit def pairRDDToRichPairRDDFunctions[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)])
    : RichPairRDDFunctions[K, V] = new RichPairRDDFunctions[K, V](rdd)
}

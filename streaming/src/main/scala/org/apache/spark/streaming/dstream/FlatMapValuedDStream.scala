/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.dstream

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.event.Event
import org.apache.spark.streaming.{Dependency, Duration, EventDependency}

import scala.reflect.ClassTag

private[streaming]
class FlatMapValuedDStream[K: ClassTag, V: ClassTag, U: ClassTag](
    parent: DStream[(K, V)],
    flatMapValueFunc: V => TraversableOnce[U]
  ) extends DStream[(K, U)](parent.ssc) {

  override def dependencies: List[Dependency[_]] = List(new EventDependency[(K, V)](parent))

  override def slideDuration: Duration = parent.slideDuration

  override def compute(event: Event): Option[RDD[(K, U)]] = {
    dependencies.flatMap(_.rdds(event)).headOption
      .map(_.asInstanceOf[RDD[(K, V)]].flatMapValues[U](flatMapValueFunc))
  }
}

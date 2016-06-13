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
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, _}
import org.apache.spark.streaming.event.Event

import scala.reflect.ClassTag

private[streaming]
abstract class WindowedDStream[T: ClassTag](
    parent: DStream[T]
  ) extends DStream[T](parent.ssc) {

  // Persist parent level by default, as those RDDs are going to be obviously reused.
  parent.persist(StorageLevel.MEMORY_ONLY_SER)

  def windowDuration: Duration

  override def parentRememberDuration: Duration = rememberDuration + windowDuration

  override def persist(level: StorageLevel): DStream[T] = {
    // Do not let this windowed DStream be persisted as windowed (union-ed) RDDs share underlying
    // RDDs and persisting the windowed RDDs would store numerous copies of the underlying data.
    // Instead control the persistence of the parent DStream.
    parent.persist(level)
    this
  }

  def shouldCompute(event: Event): Boolean

  override def compute(event: Event): Option[RDD[T]] = {
    val rddsInWindow = dependencies.head.asInstanceOf[Dependency[T]].rdds(event)
    if (rddsInWindow.nonEmpty && shouldCompute(event)) {
      Some(ssc.sc.union(rddsInWindow))
    } else {
      None
    }
  }
}

private[streaming]
class TailWindowedDStream[T: ClassTag](
    parent: DStream[T],
    windowLength: Int,
    slideLength: Int,
    skipLength: Int
  ) extends WindowedDStream[T](parent) {

  override def dependencies: List[Dependency[_]] = {
    List(new TailDependency[T](parent, skipLength, windowLength, computeEvent = true))
  }

  override def slideDuration: Duration = parent.slideDuration * slideLength

  override def windowDuration: Duration = parent.slideDuration * windowLength

  override def shouldCompute(event: Event): Boolean = {
    (event.index + 1) % slideLength == 0
  }
}

private[streaming]
class TimeWindowedDStream[T: ClassTag](
    parent: DStream[T],
    windowLength: Duration,
    slideLength: Duration
  ) extends WindowedDStream[T](parent) {

  override def dependencies: List[Dependency[_]] = {
    List(new TimeWindowDependency[T](parent, parent.zeroTime, windowLength, slideLength))
  }

  override def slideDuration: Duration = slideLength

  override def windowDuration: Duration = windowLength

  override def shouldCompute(event: Event): Boolean = {
    // A window is computed if 'event' is the only event after it.
    dependencies.head.asInstanceOf[TimeWindowDependency[T]]
      .eventsAfterLatestWindow(event).size == 1
  }
}
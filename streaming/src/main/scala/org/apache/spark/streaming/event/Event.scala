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

package org.apache.spark.streaming.event

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.streaming.Time

abstract class Event(
    val eventSource: EventSource,
    val index: Long,
    val time: Time = Time(System.currentTimeMillis)
  ) extends Serializable {

  val instanceId: Long = Event.nextInstanceId

  //override def hashCode(): Int = {
  //  ScalaRunTime._hashCode((eventSource.toProduct, index, time))
  //}

  //override def equals(other: Any): Boolean = {
  //  other match {
  //    case otherEvent: Event =>
  //      (instanceId == otherEvent.instanceId) ||
  //        (eventSource.equals(otherEvent.eventSource) &&
  //          index == otherEvent.index &&
  //          time.equals(otherEvent.time))
  //    case _ => false
  //  }
  //}

}

private[streaming] object Event {
  val instanceIdCounter: AtomicLong = new AtomicLong(0)
  def nextInstanceId: Long = instanceIdCounter.getAndIncrement()
  implicit val ordering = Time.ordering.on((event: Event) => event.time)
}


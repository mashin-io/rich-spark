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

package org.apache.spark.streaming.ui

import org.apache.spark.streaming.event.{EventSource, Event}

private[ui] case class EventSourceUIData(
    val name: String,
    val eventSourceType: String,
    val details: String,
    var status: String = "Started",
    var numOfBatches: Long = 0,
    var firstBatchEvent: Option[Event] = None,
    var lastBatchEvent: Option[Event] = None
  ) {

  def setStarted(): EventSourceUIData = {
    status = "Started"
    this
  }

  def setStopped(): EventSourceUIData = {
    status = "Stopped"
    this
  }

  def incrementBatches(): EventSourceUIData = {
    numOfBatches += 1
    this
  }

  def setFirstBatchEvent(batchEvent: Event): EventSourceUIData = {
    if (firstBatchEvent.isEmpty || firstBatchEvent.forall(_.time > batchEvent.time)) {
      firstBatchEvent = Some(batchEvent)
    }
    this
  }

  def setLastBatchEvent(batchEvent: Event): EventSourceUIData = {
    if (lastBatchEvent.isEmpty || lastBatchEvent.forall(_.time < batchEvent.time)) {
      lastBatchEvent = Some(batchEvent)
    }
    this
  }
}

private[ui] object EventSourceUIData {
  def from(eventSource: EventSource): EventSourceUIData = {
    EventSourceUIData(eventSource.name,
      eventSource.getClass.getSimpleName,
      eventSource.toDetailedString)
  }
}

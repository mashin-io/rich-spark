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

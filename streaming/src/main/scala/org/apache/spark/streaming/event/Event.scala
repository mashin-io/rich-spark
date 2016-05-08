
package org.apache.spark.streaming.event

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.util.ListenerBus

abstract class Event(
    val eventSource: EventSource,
    val time: Long = System.currentTimeMillis,
    val instanceId: Long = Event.nextInstanceId) {

  override def hashCode(): Int = instanceId.hashCode()

  override def equals(other: Any): Boolean = {
    other match {
      case otherEvent: Event => instanceId == otherEvent.instanceId
      case _ => false
    }
  }

}

private[streaming] object Event {
  val instanceIdCounter: AtomicLong = new AtomicLong(0)
  def nextInstanceId: Long = instanceIdCounter.getAndIncrement()
}

trait EventListener {
  def onEvent(event: Event)
}

private[streaming] class EventListenerBus extends ListenerBus[EventListener, Event] {
  override def doPostEvent(listener: EventListener, event: Event): Unit = {
    listener.onEvent(event)
  }
}

abstract class EventSource(ssc: StreamingContext) {
  def post(event: Event): Unit = {
    ssc.scheduler.eventBus.postToAll(event)
  }
  def start()
  def stop()
}


package org.apache.spark.streaming.event

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.{Time, StreamingContext}
import org.apache.spark.util.ListenerBus

import scala.runtime.ScalaRunTime

abstract class Event(
    val eventSource: EventSource,
    val index: Long,
    val time: Time = Time(System.currentTimeMillis)
  ) extends Serializable {

  val instanceId: Long = Event.nextInstanceId

  override def hashCode(): Int = {
    ScalaRunTime._hashCode((eventSource.toProduct, index, time))
  }

  override def equals(other: Any): Boolean = {
    other match {
      case otherEvent: Event =>
        (instanceId == otherEvent.instanceId) ||
          (eventSource.equals(otherEvent.eventSource) &&
            index == otherEvent.index &&
            time.equals(otherEvent.time))
      case _ => false
    }
  }

}

private[streaming] object Event {
  val instanceIdCounter: AtomicLong = new AtomicLong(0)
  def nextInstanceId: Long = instanceIdCounter.getAndIncrement()
  implicit val ordering = Time.ordering.on((event: Event) => event.time)
}

trait EventListener {
  def onEvent(event: Event)
}

private[streaming] class EventListenerBus extends ListenerBus[EventListener, Event] {
  override def doPostEvent(listener: EventListener, event: Event): Unit = {
    listener.onEvent(event)
  }
}
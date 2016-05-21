
package org.apache.spark.streaming.event

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.{Time, StreamingContext}
import org.apache.spark.util.ListenerBus

abstract class Event(
    val eventSource: EventSource,
    val index: Long,
    val time: Time = Time(System.currentTimeMillis)
  ) extends Serializable {

  val instanceId: Long = Event.nextInstanceId

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

abstract class EventSource(
    @transient private[streaming] var ssc: StreamingContext
  ) extends Logging with Serializable {

  @transient lazy val context = {
    if (ssc != null) {
      ssc
    } else {
      throw new SparkException(s"Streaming context for EventSource ($this) is null;" +
        s" possibly it is not initialized correctly after reloading from checkpoint.")
    }
  }

  private[streaming] def setContext(ssc: StreamingContext): Unit = {
    this.ssc = ssc
  }

  final def post(event: Event): Unit = {
    context.scheduler.jobGenerator.eventBus.postToAll(event)
  }

  def start()
  def restart()
  def stop()

  def between(from: Time, to: Time): Seq[Event] = Seq.empty

}


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


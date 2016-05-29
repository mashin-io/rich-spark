package org.apache.spark.streaming.event

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.{Time, StreamingContext}

import scala.runtime.ScalaRunTime

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

  def post(event: Event): Unit = {
    context.scheduler.jobGenerator.eventBus.postToAll(event)
  }

  def start()
  def restart()
  def stop()

  def between(from: Time, to: Time): Seq[Event] = Seq.empty

  def toProduct: Product

  override def hashCode(): Int = {
    ScalaRunTime._hashCode(toProduct)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case o: EventSource => ScalaRunTime._equals(toProduct, o.toProduct)
      case _ => false
    }
  }

}

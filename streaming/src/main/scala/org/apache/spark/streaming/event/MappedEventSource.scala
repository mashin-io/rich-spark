package org.apache.spark.streaming.event

import org.apache.spark.streaming.Time

private[streaming] case class MappedEvent(source: EventSource, event: Event)
  extends Event(source, event.index, event.time)

private[streaming] class MappedEventSource(
    prev: EventSource,
    f: Event => Event
  ) extends EventSource(prev.ssc, "MappedEventSource") {

  val cleanF = ssc.sc.clean(f)

  prev.addListener(new EventListener {
    override def onEvent(event: Event): Unit = {
      post(MappedEvent(MappedEventSource.this, cleanF(event)))
    }
  })

  override def dependencies: List[EventSource] = List(prev)

  override def start(): Unit = prev.start()

  override def stop(): Unit = prev.stop()

  override def restart(): Unit = prev.restart()

  override def between(from: Time, to: Time): Seq[Event] = {
    prev.between(from, to).map(e => MappedEvent(this, cleanF(e)))
  }

  override def toProduct: Product = (name, prev.toProduct)

  override def toDetailedString: String = {
    s"${getClass.getSimpleName}(${prev.toDetailedString})"
  }
}

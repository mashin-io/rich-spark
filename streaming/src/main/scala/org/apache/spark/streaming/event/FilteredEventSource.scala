package org.apache.spark.streaming.event

import org.apache.spark.streaming.Time

private[streaming] case class FilteredEvent(source: EventSource, event: Event)
  extends Event(source, event.index, event.time)

private[streaming] class FilteredEventSource(
    prev: EventSource,
    filterFunc: Event => Boolean
  ) extends EventSource(prev.ssc, "FilteredEventSource") {

  prev.addListener(new EventListener {
    override def onEvent(event: Event): Unit = {
      if (filterFunc(event)) {
        post(FilteredEvent(FilteredEventSource.this, event))
      }
    }
  })

  override def dependencies: List[EventSource] = List(prev)

  override def start(): Unit = prev.start()

  override def stop(): Unit = prev.stop()

  override def restart(): Unit = prev.restart()

  override def between(from: Time, to: Time): Seq[Event] = {
    prev.between(from, to).filter(filterFunc).map(e => FilteredEvent(this, e))
  }

  override def toProduct: Product = (name, prev.toProduct)
}

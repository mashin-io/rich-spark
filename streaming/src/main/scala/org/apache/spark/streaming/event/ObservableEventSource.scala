package org.apache.spark.streaming.event

import java.util.concurrent.atomic.AtomicLong

import scala.reflect.ClassTag

import rx.lang.scala.observables.ConnectableObservable
import rx.lang.scala.{Observable, Subscription}

import org.apache.spark.streaming.StreamingContext

case class ObservableEvent[T](source: EventSource, data: T, override val index: Long)
  extends Event(source, index)

final class ObservableEventSource[T: ClassTag](
    @transient ssc: StreamingContext,
    observableGenerator: => Observable[T],
    override val name: String
  ) extends EventSource(ssc, name) {

  @transient private var observable: Option[ConnectableObservable[Event]] = None
  @transient private var connectionSubscription: Option[Subscription] = None
  @transient private var observerSubscription: Option[Subscription] = None

  private val indexCounter = new AtomicLong(0L)

  override def start(): Unit = {
    require(observable.isEmpty || !connectionSubscription.exists(!_.isUnsubscribed),
      s"Observable event source $name already started")
    observable = Some(generateObservable.publish)
    observerSubscription = observable.map(_.subscribe(event => post(event)))
    connectionSubscription = observable.map(_.connect)
    logDebug(s"Observable event source $name started")
  }

  override def stop(): Unit = {
    require(observable.nonEmpty && connectionSubscription.nonEmpty,
      s"Observable event source $name is not yet started to be stopped")
    require(connectionSubscription.exists(!_.isUnsubscribed),
      s"Observable event source $name already stopped")
    connectionSubscription.foreach(_.unsubscribe())
    observerSubscription.foreach(_.unsubscribe())
    logDebug(s"Observable event source $name stopped")
  }

  override def restart(): Unit = start()

  override def toProduct: Product = ("ObservableEventSource", name)

  private def generateObservable: Observable[Event] = {
    observableGenerator.map[Event](data => ObservableEvent[T](this, data, nextIndex))
  }

  private def nextIndex: Long = indexCounter.getAndIncrement()

}

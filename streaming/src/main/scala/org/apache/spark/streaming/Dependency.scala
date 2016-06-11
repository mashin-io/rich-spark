
package org.apache.spark.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.event.Event

import scala.reflect.ClassTag

abstract class Dependency[T: ClassTag](val stream: DStream[T]) {
  def rdds(event: Event): Seq[RDD[T]]
}

class EventDependency[T: ClassTag](
    override val stream: DStream[T]
  ) extends Dependency[T](stream) {
  override def rdds(event: Event): Seq[RDD[T]] = {
    stream.getOrCompute(event).map(Seq[RDD[T]](_)).getOrElse(Seq[RDD[T]]())
  }
}

class TailDependency[T: ClassTag](
    override val stream: DStream[T],
    val skip: Int,
    val size: Int,
    val computeEvent: Boolean
  ) extends Dependency[T](stream) {
  override def rdds(event: Event): Seq[RDD[T]] = {
    val events = if (computeEvent) {
      stream.getOrCompute(event)
      stream.generatedEvents.to(event)
    } else {
      stream.generatedEvents.until(event)
    }

    events.dropRight(skip).takeRight(size)
      .map(stream.getOrCompute)
      .filter(_.nonEmpty).map(_.get).toSeq
    //Some(stream.ssc.sc.union(tailRDDs.toSeq))
  }
}

class TimeWindowDependency[T: ClassTag](
    override val stream: DStream[T],
    val zeroTime: Time,
    val windowDuration: Duration,
    val slideDuration: Duration
  ) extends Dependency[T](stream) {
  override def rdds(event: Event): Seq[RDD[T]] = {
    stream.getOrCompute(event)

    // - a window is triggered by the first event after it and cannot
    // be triggered by events at its end; it could be possible that two
    // events occur simultaneously at the end boundary of the window.
    // - two subsequent events could be more than one window slide apart.

    // the end and start times of the window just before the window of the
    // given event that has events (i.e. non-empty)
    val latestEnd = latestWindowEnd(event)
    val latestStart = latestEnd - windowDuration

    stream.generatedEvents
      .filter(e => e.time >= latestStart && e.time < latestEnd)
      .map(stream.getOrCompute)
      .filter(_.nonEmpty).map(_.get).toSeq
  }

  /**
   * Return the end time of the last window covering this event.
   */
  def lastWindowEnd(event: Event): Time = {
    val firstEnd = firstWindowEnd(event)
    firstEnd + slideDuration *
      math.floor((windowDuration - (firstEnd - event.time)) / slideDuration).toInt
  }

  /**
   * Return the end time of the first window covering this event.
   */
  def firstWindowEnd(event: Event): Time = {
    zeroTime + slideDuration * math.ceil((event.time - zeroTime) / slideDuration).toInt
  }

  /**
   * Return the end time of the window just before the window of
   * the given event and does not necessarily have events.
   */
  def prevWindowEnd(event: Event): Time = {
    firstWindowEnd(event) - slideDuration
  }

  /**
   * Return the end time of the window just before the window of the
   * given event that has events (i.e. non-empty).
   */
  def latestWindowEnd(event: Event): Time = {
    // the end time of the window just before the window of the given event
    // and does not necessarily have events
    val prevEnd = prevWindowEnd(event)
    // the end time of the window just before the window of the
    // given event that has events (i.e. non-empty)
    stream.generatedEvents
      .filter(_.time < prevEnd).lastOption.map(firstWindowEnd)
      .getOrElse(zeroTime)
  }

  /**
   * Return the sequence of events after the non-empty window
   * occurring before the given event.
   */
  def eventsAfterLatestWindow(event: Event): Seq[Event] = {
    val windowEnd = latestWindowEnd(event)
    stream.generatedEvents.filter(_.time >= windowEnd).toSeq
  }
}
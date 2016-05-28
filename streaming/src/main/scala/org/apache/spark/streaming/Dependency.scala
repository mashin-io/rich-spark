
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
      .map(e => stream.getOrCompute(e))
      .filter(_.nonEmpty).map(_.get).toSeq
    //Some(stream.ssc.sc.union(tailRDDs.toSeq))
  }
}
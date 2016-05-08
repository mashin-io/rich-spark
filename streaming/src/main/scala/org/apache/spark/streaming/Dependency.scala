
package org.apache.spark.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.event.Event

import scala.reflect.ClassTag

abstract class Dependency[T: ClassTag](val stream: DStream[T]) {
  def rdd(event: Event): Option[RDD[T]]
}

class EventDependency[T: ClassTag](
    override val stream: DStream[T]
  ) extends Dependency[T](stream) {
  override def rdd(event: Event): Option[RDD[T]] = stream.getOrCompute(event)
}

class TailDependency[T: ClassTag](
    override val stream: DStream[T],
    val skip: Int,
    val size: Int
  ) extends Dependency[T](stream) {
  override def rdd(event: Event): Option[RDD[T]] = {
    if (event != null) {
      stream.getOrCompute(event)
    }
    val tailRDDs = stream.generatedRDDs.values
      .dropRight(skip).takeRight(size)
    Some(stream.ssc.sc.union(tailRDDs.toSeq))
  }
}
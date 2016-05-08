
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

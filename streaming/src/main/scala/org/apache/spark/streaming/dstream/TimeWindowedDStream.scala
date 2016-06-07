package org.apache.spark.streaming.dstream

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.event.Event
import org.apache.spark.streaming.{Dependency, Duration, TimeWindowDependency}

import scala.reflect.ClassTag

private[streaming]
class TimeWindowedDStream[T: ClassTag](
    parent: DStream[T],
    _window: Duration,
    _slide: Duration
  ) extends DStream[T](parent.ssc) {

  // Persist parent level by default, as those RDDs are going to be obviously reused.
  parent.persist(StorageLevel.MEMORY_ONLY_SER)

  def windowLength: Duration = _window

  def slideLength: Duration = _slide

  override def dependencies: List[Dependency[_]] = {
    List(new TimeWindowDependency[T](parent, parent.zeroTime, windowLength, slideLength))
  }

  override def slideDuration: Duration = slideLength

  override def parentRememberDuration: Duration = rememberDuration + windowLength

  override def persist(level: StorageLevel): DStream[T] = {
    // Do not let this windowed DStream be persisted as windowed (union-ed) RDDs share underlying
    // RDDs and persisting the windowed RDDs would store numerous copies of the underlying data.
    // Instead control the persistence of the parent DStream.
    parent.persist(level)
    this
  }

  override def compute(event: Event): Option[RDD[T]] = {
    // Compute the window that ends at or just before 'event'.
    val dependency = dependencies.head.asInstanceOf[TimeWindowDependency[T]]
    val rddsInWindow = dependency.rdds(event)
    // A window is computed if 'event' is the only event at the end or after it.
    if (rddsInWindow.nonEmpty && dependency.eventsAfterPrevWindow(event).size <= 1) {
      Some(ssc.sc.union(rddsInWindow))
    } else {
      None
    }
  }
}

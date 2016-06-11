package org.apache.spark.streaming.dstream

import org.apache.spark.Partitioner
import org.apache.spark.rdd.{CoGroupedRDD, RDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.event.Event
import org.apache.spark.streaming.{TimeWindowDependency, Duration, TailDependency, Dependency}

import scala.reflect.ClassTag

private[streaming]
class ReducedTimeWindowedDStream[K: ClassTag, V: ClassTag](
    parent: DStream[(K, V)],
    reduceFunc: (V, V) => V,
    invReduceFunc: (V, V) => V,
    filterFunc: Option[((K, V)) => Boolean],
    _window: Duration,
    _slide: Duration,
    partitioner: Partitioner
  ) extends DStream[(K, V)](parent.ssc) {

  // Reduce each batch of data using reduceByKey which will be further reduced by window
  // by ReducedTimeWindowedDStream
  private val reducedStream = parent.reduceByKey(reduceFunc, partitioner)

  // Persist RDDs to memory by default as these RDDs are going to be reused.
  super.persist(StorageLevel.MEMORY_ONLY_SER)
  reducedStream.persist(StorageLevel.MEMORY_ONLY_SER)

  def windowLength: Duration = _window

  def slideLength: Duration = _slide

  override def dependencies: List[Dependency[_]] = {
    //  _____________________________
    // |  previous window   _________|___________________
    // |___________________|       current window        |  --------------> Time
    //                     |_____________________________|
    //
    // |________ _________|          |________ _________|
    //          |                             |
    //          V                             V
    //       old RDDs                     new RDDs
    //
    val length = windowLength min slideLength
    val oldZeroTime = parent.zeroTime - slideLength
    val newZeroTime = parent.zeroTime + windowLength - length

    List(new TailDependency[(K, V)](this, skip = 0, size = 1, computeEvent = false),
      new TimeWindowDependency[(K, V)](reducedStream, oldZeroTime, length, slideLength),
      new TimeWindowDependency[(K, V)](reducedStream, newZeroTime, length, slideLength))
  }

  override def slideDuration: Duration = slideLength

  override val mustCheckpoint = true

  override def parentRememberDuration: Duration = rememberDuration + windowLength

  override def persist(storageLevel: StorageLevel): DStream[(K, V)] = {
    super.persist(storageLevel)
    reducedStream.persist(storageLevel)
    this
  }

  override def checkpoint(interval: Duration): DStream[(K, V)] = {
    super.checkpoint(interval)
    // reducedStream.checkpoint(interval)
    this
  }

  override def compute(event: Event): Option[RDD[(K, V)]] = {
    val reduceF = reduceFunc
    val invReduceF = invReduceFunc

    val dependenciesList = dependencies
    // Make the list of RDDs that needs to be cogrouped together for reducing their reduced values
    val allRDDs = dependenciesList.map {
      case dependency: TailDependency[(K, V)] =>
        // Get the RDD of the reduced value of the previous window
        val previousWindowRDD = dependency.rdds(event)
        if (previousWindowRDD.isEmpty) {
          Seq(ssc.sc.makeRDD(Seq[(K, V)]()))
        } else {
          previousWindowRDD
        }
      case dependency: TimeWindowDependency[(K, V)] => dependency.rdds(event)
    }

    // A window is computed if 'event' is the only event after it.
    val newRDDsDependency = dependenciesList.last.asInstanceOf[TimeWindowDependency[(K, V)]]
    if (newRDDsDependency.eventsAfterLatestWindow(event).size == 1) {
      // Get the RDDs of the reduced values in "old time steps"
      val oldRDDs = allRDDs(1)
      // Get the RDDs of the reduced values in "new time steps"
      val newRDDs = allRDDs(2)

      // Cogroup the reduced RDDs and merge the reduced values
      val cogroupedRDD = new CoGroupedRDD[K](
        allRDDs.flatMap(s => s).toSeq.asInstanceOf[Seq[RDD[(K, _)]]],
        partitioner)
      // val mergeValuesFunc = mergeValues(oldRDDs.size, newRDDs.size) _

      val numOldValues = oldRDDs.size
      val numNewValues = newRDDs.size

      val mergeValues = (arrayOfValues: Array[Iterable[V]]) => {
        if (arrayOfValues.length != 1 + numOldValues + numNewValues) {
          throw new Exception("Unexpected number of sequences of reduced values")
        }
        // Getting reduced values "old time steps" that will be removed from current window
        val oldValues = (1 to numOldValues).map(i => arrayOfValues(i)).filter(_.nonEmpty).map(_.head)
        // Getting reduced values "new time steps"
        val newValues =
          (1 to numNewValues).map(i => arrayOfValues(numOldValues + i)).filter(_.nonEmpty).map(_.head)

        if (arrayOfValues(0).isEmpty) {
          // If previous window's reduce value does not exist, then at least new values should exist
          if (newValues.isEmpty) {
            throw new Exception("Neither previous window has value for key, nor new values found. " +
              "Are you sure your key class hashes consistently?")
          }
          // Reduce the new values
          newValues.reduce(reduceF) // return
        } else {
          // Get the previous window's reduced value
          var tempValue = arrayOfValues(0).head
          // If old values exists, then inverse reduce them from previous value
          if (oldValues.nonEmpty) {
            tempValue = invReduceF(tempValue, oldValues.reduce(reduceF))
          }
          // If new values exists, then reduce them with previous value
          if (newValues.nonEmpty) {
            tempValue = reduceF(tempValue, newValues.reduce(reduceF))
          }
          tempValue // return
        }
      }

      val mergedValuesRDD = cogroupedRDD.asInstanceOf[RDD[(K, Array[Iterable[V]])]]
        .mapValues(mergeValues)

      if (filterFunc.isDefined) {
        Some(mergedValuesRDD.filter(filterFunc.get))
      } else {
        Some(mergedValuesRDD)
      }
    } else {
      None
    }
  }
}

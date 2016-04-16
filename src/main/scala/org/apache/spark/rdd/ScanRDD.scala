/*
 * Copyright (c) 2016 Mashin (http://mashin.io). All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.annotation.DeveloperApi

import scala.reflect.ClassTag

class ScanRDD[T: ClassTag](
    val prev: RDD[T],
    val cleanF: (T, T) => T,
    val initials: Array[T],
    val initialPartitionIndex: Int,
    val isScanLeft: Boolean = true)
  extends RDD[T](prev) {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val iter = if (isScanLeft) {
      prev.iterator(split, context)
        .map(cleanF(initials(split.index), _))
    } else {
      prev.iterator(split, context)
        .map(cleanF(_, initials(split.index)))
    }
    if (split.index == initialPartitionIndex) {
      iter
    } else {
      if (isScanLeft) {
        iter.drop(1)
      } else {
        iter.toIndexedSeq.init.iterator
      }
    }
  }

  override protected def getPartitions: Array[Partition] = {
    prev.partitions
  }
}

object ScanRDD {

  def scanLeft[T: ClassTag](
      rdd: RDD[T],
      zero: T,
      init: T,
      f: (T, T) => T)
    : RDD[T] = {

    val scanPartition: Iterator[T] => Iterator[T] = iter => iter.scanLeft(zero)(f)

    val partiallyScannedRdd = rdd.mapPartitions(scanPartition, preservesPartitioning = true)

    val lastScanOfPartition: Iterator[T] => Option[T] = iter => iter.toIndexedSeq.lastOption

    var initialsOptions: Array[(Option[T], Int)] = Array((Some(init), 0))
    val appendResults = (index: Int, taskResult: Option[T]) => {
      initialsOptions ++= Array((taskResult, index + 1))
    }

    val sc = rdd.sparkContext

    sc.runJob(partiallyScannedRdd, lastScanOfPartition, appendResults)

    val fWithOptions: (Option[T], Option[T]) => Option[T] = (v1, v2) => {
      (v1, v2) match {
        case (Some(t1), Some(t2)) => Some(f(t1, t2))
        case (Some(t1), None) => v1
        case _ => v2
      }
    }

    val initials = initialsOptions.sortWith((a, b) => a._2 < b._2)
      .map(t => t._1)
      .scanLeft(None.asInstanceOf[Option[T]])(fWithOptions)
      .tail
      .map(_.getOrElse(zero))

    new ScanRDD[T](partiallyScannedRdd, sc.clean(f),
      initials.init, initialPartitionIndex = 0, isScanLeft = true)
  }

  def scanRight[T: ClassTag](
      rdd: RDD[T],
      zero: T,
      init: T,
      f: (T, T) => T)
    : RDD[T] = {

    val scanPartition: Iterator[T] => Iterator[T] = iter => iter.scanRight(zero)(f)

    val partiallyScannedRdd = rdd.mapPartitions(scanPartition, preservesPartitioning = true)

    val lastScanOfPartition: Iterator[T] => Option[T] = iter => iter.toIndexedSeq.headOption

    var initialsOptions: Array[(Option[T], Int)] = Array.empty
    val appendResults = (index: Int, taskResult: Option[T]) => {
      initialsOptions ++= Array((taskResult, index - 1))
    }

    val sc = rdd.sparkContext

    sc.runJob(partiallyScannedRdd, lastScanOfPartition, appendResults)

    initialsOptions ++= Array((Some(init), initialsOptions.length - 1))

    val fWithOptions: (Option[T], Option[T]) => Option[T] = (v1, v2) => {
      (v1, v2) match {
        case (Some(t1), Some(t2)) => Some(f(t1, t2))
        case (Some(t1), None) => v1
        case _ => v2
      }
    }

    val initials = initialsOptions.sortWith((a, b) => a._2 < b._2)
      .map(t => t._1)
      .scanRight(None.asInstanceOf[Option[T]])(fWithOptions)
      .init
      .map(_.getOrElse(zero))

    new ScanRDD[T](partiallyScannedRdd, sc.clean(f),
      initials.tail, initialPartitionIndex = initials.length - 2, isScanLeft = false)
  }

}

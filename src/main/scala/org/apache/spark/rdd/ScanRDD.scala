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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}

import scala.reflect.ClassTag

private[spark] class ScanPartition(val i: Partition, val prefixes: Seq[Partition])
  extends Partition {
  override def index: Int = i.index
}

class ScanRDD[T: ClassTag, U: ClassTag](
    val partiallyScannedRdd: RDD[U],
    val prefixesRdd: RDD[Option[U]],
    val cleanC: (U, U) => U,
    val isScanLeft: Boolean = true)
  extends RDD[U](partiallyScannedRdd.sparkContext,
    List(new OneToOneDependency[U](partiallyScannedRdd),
      new OneToOneDependency[Option[U]](prefixesRdd))) {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[U] = {
    val scanSplit = split.asInstanceOf[ScanPartition]

    val cWithOptions: (Option[U], Option[U]) => Option[U] = (v1, v2) => {
      (v1, v2) match {
        case (Some(t1), Some(t2)) => Some(cleanC(t1, t2))
        case (Some(t1), None) => v1
        case _ => v2
      }
    }

    val prefixes = scanSplit.prefixes.map(prefixesRdd.iterator(_, context).toSeq.head)

    val prefix = (isScanLeft, prefixes) match {
        case (_, Nil) => None
        case (_, head :: Nil) => head
        case (true, _) => prefixes.reduceLeft(cWithOptions)
        case (false, _) => prefixes.reduceRight(cWithOptions)
      }

    (isScanLeft, prefix) match {
      case (true, Some(p)) => partiallyScannedRdd.iterator(scanSplit.i, context).map(cleanC(p, _))
      case (false, Some(p)) => partiallyScannedRdd.iterator(scanSplit.i, context).map(cleanC(_, p))
      case (_, None) => partiallyScannedRdd.iterator(scanSplit.i, context)
    }
  }

  override protected def getPartitions: Array[Partition] = {
    if (isScanLeft) {
      partiallyScannedRdd.partitions.map(p =>
        new ScanPartition(p, prefixesRdd.partitions.slice(0, p.index)))
    } else {
      partiallyScannedRdd.partitions.map(p =>
        new ScanPartition(p, prefixesRdd.partitions.drop(p.index + 1)))
    }
  }

}

object ScanRDD {

  def scanLeft[T: ClassTag, U: ClassTag](
      rdd: RDD[T],
      zero: U,
      init: U,
      f: (U, T) => U,
      c: (U, U) => U)
    : RDD[U] = {

    val partiallyScannedRdd = rdd.mapPartitionsWithIndex((i, iter) => {
        if (i == 0) {
          iter.scanLeft(init)(f)
        } else {
          iter.scanLeft(zero)(f).drop(1)
        }
      }, preservesPartitioning = true)

    val prefixRdd = partiallyScannedRdd.mapPartitions(iter => Iterator.single(iter.toSeq.lastOption))

    val sc = rdd.sparkContext

    new ScanRDD[T, U](partiallyScannedRdd, prefixRdd, sc.clean(c), isScanLeft = true)
  }

  def scanLeft[T: ClassTag](
      rdd: RDD[T],
      zero: T,
      init: T,
      f: (T, T) => T)
    : RDD[T] = {
    scanLeft[T, T](rdd, zero, init, f, f)
  }

  def scanRight[T: ClassTag, U: ClassTag](
      rdd: RDD[T],
      zero: U,
      init: U,
      f: (T, U) => U,
      c: (U, U) => U)
    : RDD[U] = {

    val lastPartition = rdd.partitions.length - 1

    val partiallyScannedRdd = rdd.mapPartitionsWithIndex((i, iter) => {
      if (i == lastPartition) {
        iter.scanRight(init)(f)
      } else {
        iter.scanRight(zero)(f).toSeq.dropRight(1).iterator
      }
    }, preservesPartitioning = true)

    val prefixRdd = partiallyScannedRdd.mapPartitions(iter => Iterator.single(iter.toSeq.headOption))

    val sc = rdd.sparkContext

    new ScanRDD[T, U](partiallyScannedRdd, prefixRdd, sc.clean(c), isScanLeft = false)
  }

  def scanRight[T: ClassTag](
      rdd: RDD[T],
      zero: T,
      init: T,
      f: (T, T) => T)
    : RDD[T] = {
    scanRight[T, T](rdd, zero, init, f, f)
  }

}

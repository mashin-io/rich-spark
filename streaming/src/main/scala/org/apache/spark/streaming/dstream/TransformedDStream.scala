/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.dstream

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.event.Event
import org.apache.spark.streaming.{Time, Dependency, Duration, EventDependency}

import scala.reflect.ClassTag

private[streaming] abstract class TransformFunction[U, EventOrTime](
    func: (Seq[RDD[_]], EventOrTime) => RDD[U])
private[streaming] case class TransformFunctionWithEvent[U](
    func: (Seq[RDD[_]], Event) => RDD[U]) extends TransformFunction[U, Event](func)
private[streaming] case class TransformFunctionWithTime[U](
    func: (Seq[RDD[_]], Time) => RDD[U]) extends TransformFunction[U, Time](func)

private[streaming]
class TransformedDStream[U: ClassTag] (
    parents: Seq[DStream[_]],
    transformFunc: TransformFunction[U, _]
  ) extends DStream[U](parents.head.ssc) {

  require(parents.nonEmpty, "List of DStreams to transform is empty")
  require(parents.map(_.ssc).distinct.size == 1, "Some of the DStreams have different contexts")
  require(parents.map(_.slideDuration).distinct.size == 1,
    "Some of the DStreams have different slide durations")

  override def dependencies: List[Dependency[_]] = parents.toList.map(new EventDependency(_))

  override def slideDuration: Duration = parents.head.slideDuration

  override def compute(event: Event): Option[RDD[U]] = {
    val parentRDDs = dependencies.map(_.rdds(event).headOption).map { _.getOrElse(
      // Guard out against parent DStream that return None instead of Some(rdd) to avoid NPE
      throw new SparkException(s"Couldn't generate RDD from parent at event $event"))
    }
    val transformedRDD = transformFunc match {
      case transformFuncWithEvent: TransformFunctionWithEvent[U] =>
        transformFuncWithEvent.func(parentRDDs, event)
      case transformFuncWithTime: TransformFunctionWithTime[U] =>
        transformFuncWithTime.func(parentRDDs, event.time)
    }
    if (transformedRDD == null) {
      throw new SparkException("Transform function must not return null. " +
        "Return SparkContext.emptyRDD() instead to represent no element " +
        "as the result of transformation.")
    }
    Some(transformedRDD)
  }

  /**
   * Wrap a body of code such that the call site and operation scope
   * information are passed to the RDDs created in this body properly.
   * This has been overridden to make sure that `displayInnerRDDOps` is always `true`, that is,
   * the inner scopes and callsites of RDDs generated in `DStream.transform` are always
   * displayed in the UI.
   */
  override protected[streaming] def createRDDWithLocalProperties[U](
      event: Event,
      displayInnerRDDOps: Boolean)(body: => U): U = {
    super.createRDDWithLocalProperties(event, displayInnerRDDOps = true)(body)
  }
}

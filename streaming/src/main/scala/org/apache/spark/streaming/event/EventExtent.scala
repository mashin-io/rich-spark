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

package org.apache.spark.streaming.event

import scala.annotation.tailrec
import scala.collection.SortedSet
import scala.collection.mutable.{HashMap, ListBuffer}

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.Duration

private[streaming]
abstract class ExtentNode[CountOrDuration](
    extent: CountOrDuration,
    var parent: Option[ExtentNode[_]] = None
  ) extends Serializable

private[streaming]
class CountExtentNode(val count: Int) extends ExtentNode[Int](count) {
  override def toString: String = s"$count event(s)"
}

private[streaming]
class DurationExtentNode(val duration: Duration) extends ExtentNode[Duration](duration) {
  override def toString: String = s"$duration"
}

/**
 * This class represents an extent over a sequence of events on a timeline.
 * An extent describes the length of a window of contiguous events on the
 * timeline. Window length could be described by the number of events within
 * , a time duration or both such that for a sequence of events on a timeline
 * , the extent could be evaluated to the total number of events or the total
 * duration that is covered by the extent.
 *
 * An extent is described by one or more sequences of events counts and durations.
 * For example, an extent could be as simple as [2 events] or [2 seconds] or more
 * complex like [1 event -> 1 second -> 5 events] or even more complex like
 * [ [2 events] or [2 seconds] or [1 event -> 1 second -> 5 events] ].
 *
 * The multiple sequences describing an extent are all evaluated to the corresponding
 * length (either a count or a duration) and depending on a limiting criterion
 * , only one length is selected (either the maximum in case of `MaxEventExtent` or
 * the minimum in case of `MinEventExtent`).
 * {{{
 *    0--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|-->
 *    1s 2s 3s 4s 5s 6s 7s 8s 9s 10s ...
 *
 * Given this sequence of events:
 *    0--|--|----|---|-------|-->
 *      (1)(2)  (3) (4)     (5)
 *
 * Extent [2 events]:                 Extent [2 seconds]:
 *  covers events = {5, 4}              covers events = {5}
 *  evalCount = 2 events                evalCount = 1 event
 *  evalDuration = 4 seconds            evalDuration = 2 seconds
 *
 * Extent [2 events -> 2 seconds]
 *  covers events = {5, 4, 3}
 *  evalCount = 3 events
 *  evalDuration = 6 seconds
 * }}}
 */
private[streaming]
abstract class EventExtent extends Serializable with Logging {
  val roots = ListBuffer.empty[ExtentNode[_]]
  val leaves = ListBuffer.empty[ExtentNode[_]]

  def isSet: Boolean = leaves.nonEmpty

  def copy: EventExtent = {

    def copy(node: ExtentNode[_]): ExtentNode[_] = {
      node match {
        case a: CountExtentNode => new CountExtentNode(a.count)
        case b: DurationExtentNode => new DurationExtentNode(b.duration)
        case _ => null
      }
    }

    val cloned = this.getClass.newInstance().asInstanceOf[EventExtent]
    val identityMap = new HashMap[ExtentNode[_], Option[ExtentNode[_]]]()

    @tailrec def next(
        lastClone: ExtentNode[_],
        parentOption: Option[ExtentNode[_]])
      : Option[ExtentNode[_]] = parentOption match {
      case None => Some(lastClone)
      case Some(parent) if identityMap.contains(parent) =>
        lastClone.parent = identityMap(parent)
        None
      case Some(parent) =>
        val parentClone = Some(copy(parent))
        identityMap += ((parent, parentClone))
        lastClone.parent = parentClone
        next(parentClone.get, parent.parent)
    }

    leaves.foreach { leaf =>
      var leafClone = copy(leaf)
      cloned.leaves += leafClone
      next(leafClone, leaf.parent).foreach(cloned.roots +=)
    }

    cloned
  }

  def set(count: Int): Boolean = {
    val shouldSet = !leaves.exists {
      case leaf: CountExtentNode =>
        leaf.count == limitCount(Seq(leaf.count, count))
      case _ => false
    }
    if (shouldSet) {
      val extentCount = new CountExtentNode(count)
      leaves += extentCount
      roots += extentCount
    }
    shouldSet
  }

  def set(duration: Duration): Boolean = {
    val shouldSet = duration != null && !leaves.exists {
      case leaf: DurationExtentNode =>
        leaf.duration == limitDuration(Seq(leaf.duration, duration))
      case _ => false
    }
    if (shouldSet) {
      val extentDuration = new DurationExtentNode(duration)
      leaves += extentDuration
      roots += extentDuration
    }
    shouldSet
  }

  def set(extent: EventExtent): Boolean = {
    val shouldSetExtent = extent != null
    if (shouldSetExtent) {
      val cloned = extent.copy
      leaves ++= cloned.leaves
      roots ++= cloned.roots
    }
    shouldSetExtent
  }

  def + (count: Int): EventExtent = {
    val countExtentNode = Some(new CountExtentNode(count))
    val res = copy
    if (res.leaves.isEmpty) {
      res.leaves += countExtentNode.get
    } else {
      res.roots.foreach(_.parent = countExtentNode)
      res.roots.clear()
    }
    res.roots += countExtentNode.get
    res
  }

  def + (duration: Duration): EventExtent = {
    val durationExtentNode = Some(new DurationExtentNode(duration))
    val res = copy
    if (res.leaves.isEmpty) {
      res.leaves += durationExtentNode.get
    } else {
      res.roots.foreach(_.parent = durationExtentNode)
      res.roots.clear()
    }
    res.roots += durationExtentNode.get
    res
  }

  /** Select and return the limit count from the provided seq of counts */
  protected def limitCount(counts: Seq[Int]): Int

  /** Select and return the limit duration from the provided seq of durations */
  protected def limitDuration(durations: Seq[Duration]): Duration

  /** Return the number of events covered by this extent given a sorted set of events */
  def evalCount(events: SortedSet[Event]): Int = if (!isSet) 0 else {
    val limitPerNodeMap = new HashMap[ExtentNode[_], Int]

    def evalNode(node: ExtentNode[_], remainingEvents: Seq[Event]): Int = node match {
      case countExtentNode: CountExtentNode =>
        countExtentNode.count min remainingEvents.size
      case durationExtentNode: DurationExtentNode =>
        val minExtentTime = remainingEvents.last.time - durationExtentNode.duration
        remainingEvents.count(_.time > minExtentTime)
    }

    @tailrec def eval(
        nodeOption: Option[ExtentNode[_]],
        remainingEvents: Seq[Event],
        lastN: Int)
      : Int = nodeOption match {
      case None => lastN
      case Some(node) =>
        val deltaN = evalNode(node, remainingEvents)
        val n = deltaN + lastN
        val limitPerNode = limitPerNodeMap.getOrElse(node, 0)
        if (limitPerNode == limitCount(Seq(limitPerNode, n))) {
          limitPerNode
        } else {
          limitPerNodeMap += ((node, n))
          if (remainingEvents.size <= deltaN) {
            n
          } else {
            eval(node.parent, remainingEvents.view(0, remainingEvents.size - deltaN), n)
          }
        }
    }

    limitCount(leaves.map(leaf => eval(Some(leaf), events.toSeq, 0)))
  }

  /** Return the duration covered by this extent given a sorted set of events */
  def evalDuration(events: SortedSet[Event]): Duration = if (!isSet) Duration(0) else {
    val limitPerNodeMap = new HashMap[ExtentNode[_], Duration]

    def evalNode(node: ExtentNode[_], remainingEvents: Seq[Event]): (Int, Duration) = {
      val lastEvent = remainingEvents.last
      node match {
        case countExtentNode: CountExtentNode =>
          val cutIndex = if (remainingEvents.size > countExtentNode.count) {
            remainingEvents.size - countExtentNode.count
          } else {
            0
          }
          (cutIndex, lastEvent.time - remainingEvents((cutIndex - 1) max 0).time)
        case durationExtentNode: DurationExtentNode =>
          val minExtentTime = lastEvent.time - durationExtentNode.duration
          val cutIndex = remainingEvents.indexWhere(_.time > minExtentTime)
          (cutIndex, lastEvent.time - remainingEvents((cutIndex - 1) max 0).time min
            durationExtentNode.duration)
      }
    }

    @tailrec def eval(
        nodeOption: Option[ExtentNode[_]],
        remainingEvents: Seq[Event],
        lastDuration: Duration)
      : Duration = nodeOption match {
      case None => lastDuration
      case Some(node) =>
        val (cutIndex, deltaDuration) = evalNode(node, remainingEvents)
        val duration = deltaDuration + lastDuration
        val limitPerNode = limitPerNodeMap.getOrElse(node, Duration(0))
        if (limitPerNode == limitDuration(Seq(limitPerNode, duration))) {
          limitPerNode
        } else {
          limitPerNodeMap += ((node, duration))
          if (cutIndex <= 0) {
            duration
          } else {
            eval(node.parent, remainingEvents.view(0, cutIndex), duration)
          }
        }
    }

    limitDuration(leaves.map(leaf => eval(Some(leaf), events.toSeq, Duration(0))))
  }

  override def toString: String = {
    leaves.map { leaf =>
      val list = ListBuffer.empty[String]
      list += leaf.toString
      var parent = leaf.parent
      while (parent.nonEmpty) {
        list += parent.get.toString
        parent = parent.get.parent
      }
      list.mkString("[", " + ", "]")
    }.mkString(" or ")
  }

}

private[streaming]
class MaxEventExtent extends EventExtent {

  override def limitCount(counts: Seq[Int]): Int = counts.max

  override def limitDuration(durations: Seq[Duration]): Duration = {
    durations.maxBy(_.milliseconds)
  }

  override def + (count: Int): MaxEventExtent = {
    super.+(count).asInstanceOf[MaxEventExtent]
  }

  override def + (duration: Duration): MaxEventExtent = {
    super.+(duration).asInstanceOf[MaxEventExtent]
  }

}

private[streaming]
class MinEventExtent extends EventExtent {

  override def limitCount(counts: Seq[Int]): Int = counts.min

  override def limitDuration(durations: Seq[Duration]): Duration = {
    durations.minBy(_.milliseconds)
  }

  override def + (count: Int): MinEventExtent = {
    super.+(count).asInstanceOf[MinEventExtent]
  }

  override def + (duration: Duration): MinEventExtent = {
    super.+(duration).asInstanceOf[MinEventExtent]
  }

}
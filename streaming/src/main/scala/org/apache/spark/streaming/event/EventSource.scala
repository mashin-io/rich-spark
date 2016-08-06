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

import java.util.concurrent.CopyOnWriteArrayList
import java.util.function.{Consumer, Predicate}

import scala.reflect.ClassTag
import scala.runtime.ScalaRunTime
import scala.util.control.NonFatal

import rx.lang.scala.Observable

import org.apache.spark.SparkException
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.util.Utils

trait EventListener extends Serializable {
  def onEvent(event: Event)
}

abstract class EventSource(
    @transient private[streaming] var ssc: StreamingContext,
    val name: String
  ) extends Logging with Serializable {

  private val listeners = new CopyOnWriteArrayList[EventListener]

  @transient lazy val context = {
    if (ssc != null) {
      ssc
    } else {
      throw new SparkException(s"Streaming context for EventSource ($this) is null;" +
        s" possibly it is not initialized correctly after reloading from checkpoint.")
    }
  }

  private[streaming] def setContext(ssc: StreamingContext): Unit = {
    this.ssc = ssc
    dependencies.foreach(_.setContext(ssc))
  }

  def addListener(listener: EventListener): Unit = {
    listeners.add(listener)
  }

  def removeListener(listener: EventListener): Unit = {
    listeners.remove(listener)
  }

  def removeListeners[T <: EventListener : ClassTag](): Unit = {
    val c = implicitly[ClassTag[T]].runtimeClass
    listeners.removeIf(new Predicate[EventListener] {
      override def test(listener: EventListener): Boolean = {
        c.isAssignableFrom(listener.getClass)
      }
    })
  }

  def dependencies: List[EventSource] = List.empty

  def post(event: Event): Unit = {
    listeners.forEach(new Consumer[EventListener] {
      override def accept(listener: EventListener): Unit = try {
        listener.onEvent(event)
      } catch {
        case NonFatal(e) =>
          logError(s"Listener ${Utils.getFormattedClassName(listener)} threw an exception", e)
      }
    })
  }

  def start()
  def restart()
  def stop()

  def between(from: Time, to: Time): Seq[Event] = Seq.empty

  def toProduct: Product

  override def hashCode(): Int = {
    ScalaRunTime._hashCode(toProduct)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case o: EventSource => ScalaRunTime._equals(toProduct, o.toProduct)
      case _ => false
    }
  }

  override def toString: String = s"${getClass.getSimpleName}($name)"

  def toDetailedString: String = toString

  final def map(f: Event => Event): EventSource = {
    new MappedEventSource(this, f)
  }

  final def filter(filterFunc: Event => Boolean): EventSource = {
    new FilteredEventSource(this, filterFunc)
  }

  final def toObservable: Observable[Event] = {
    Observable.apply[Event](subscriber => {
      EventSource.this.addListener(new EventListener {
        override def onEvent(event: Event): Unit = {
          subscriber.onNext(event)
        }
      })
      start()
    })
  }

}

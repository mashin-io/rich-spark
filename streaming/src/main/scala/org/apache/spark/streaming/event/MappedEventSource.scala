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

import org.apache.spark.streaming.Time

private[streaming] case class MappedEvent(source: EventSource, event: Event)
  extends Event(source, event.index, event.time)

private[streaming] class MappedEventSource(
    prev: EventSource,
    f: Event => Event
  ) extends EventSource(prev.ssc, "MappedEventSource") {

  val cleanF = ssc.sc.clean(f)

  prev.addListener(new EventListener {
    override def onEvent(event: Event): Unit = {
      post(MappedEvent(MappedEventSource.this, cleanF(event)))
    }
  })

  override def dependencies: List[EventSource] = List(prev)

  override def start(): Unit = {
    try {
      prev.start()
    } catch {
      case e: Exception =>
        logWarning(s"Exception while starting dependent event source $prev", e)
    }
  }

  override def stop(): Unit = {
    try {
      prev.stop()
    } catch {
      case e: Exception =>
        logWarning(s"Exception while stopping dependent event source $prev", e)
    }
  }

  override def restart(): Unit = {
    try {
      prev.restart()
    } catch {
      case e: Exception =>
        logWarning(s"Exception while restarting dependent event source $prev", e)
    }
  }

  override def between(from: Time, to: Time): Seq[Event] = {
    prev.between(from, to).map(e => MappedEvent(this, cleanF(e)))
  }

  override def toProduct: Product = (name, cleanF, prev.toProduct)

  override def toDetailedString: String = {
    s"${getClass.getSimpleName}(${prev.toDetailedString})"
  }
}

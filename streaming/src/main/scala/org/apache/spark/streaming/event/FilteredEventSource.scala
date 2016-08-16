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

private[streaming] case class FilteredEvent(source: EventSource, event: Event)
  extends Event(source, event.index, event.time)

private[streaming] class FilteredEventSource(
    prev: EventSource,
    filterFunc: Event => Boolean
  ) extends EventSource(prev.ssc, "FilteredEventSource") {

  prev.addListener(new EventListener {
    override def onEvent(event: Event): Unit = {
      if (filterFunc(event)) {
        post(FilteredEvent(FilteredEventSource.this, event))
      }
    }
  })

  override def dependencies: List[EventSource] = List(prev)

  override def start(): Unit = prev.start()

  override def stop(): Unit = prev.stop()

  override def restart(): Unit = prev.restart()

  override def between(from: Time, to: Time): Seq[Event] = {
    prev.between(from, to).filter(filterFunc).map(e => FilteredEvent(this, e))
  }

  override def toProduct: Product = (name, filterFunc, prev.toProduct)

  override def toDetailedString: String = {
    s"${getClass.getSimpleName}(${prev.toDetailedString})"
  }
}

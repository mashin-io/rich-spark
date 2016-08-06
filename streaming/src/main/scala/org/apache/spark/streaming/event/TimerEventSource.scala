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

import org.apache.spark.streaming.{Duration, StreamingContext, Time}

case class TimerEvent(source: EventSource, override val index: Long, override val time: Time)
  extends Event(source, index, time)

class TimerEventSource(
    @transient ssc: StreamingContext,
    startTime: Time,
    endTime: Time,
    period: Duration,
    name: String
  ) extends EventSource(ssc, name) {

  private var stopped = false
  private var nextTime: Time = Time(-1)

  override def start(): Unit = synchronized {
    require(startTime < endTime,
      s"End time ($endTime ms) should be greater than start time ($startTime ms).")

    require(period.milliseconds > 0, s"Period $period should be greater than 0.")

    val currTime = Time(context.clock.getTimeMillis())
    if (startTime < currTime) {
      // if start time already elapsed, start from the next possible
      // period after current time
      nextTime = startTime + Duration(period.milliseconds *
        (1 + (currTime - startTime) / period).toLong)
    } else {
      nextTime = startTime
    }
    stopped = false
    new Thread(s"TimerEventSource $name") {
      setDaemon(true)
      override def run() { loop() }
    }.start()
    logDebug(s"Timer ($name) started, first timer event emits on $nextTime")
  }

  private def loop(): Unit = {
    val clock = context.clock
    var index = ((nextTime - startTime) / period).toLong
    while (!stopped && nextTime <= endTime) {
      clock.waitTillTime(nextTime.milliseconds)
      post(TimerEvent(this, index, nextTime))
      logDebug(s"Timer ($name) event with time $nextTime and period index $index")
      index += 1
      nextTime += period
    }
    stop()
  }

  override def restart() = start()

  override def stop(): Unit = synchronized {
    if (!stopped) {
      stopped = true
      logDebug(s"Timer ($name) stopped")
    }
  }

  override def between(from: Time, to: Time): Seq[Event] = {
    from.to(to, period)
      .filter(t => t >= startTime && t <= endTime)
      .map(t => TimerEvent(this, ((t - startTime) / period).toInt, t))
  }

  override def toProduct: Product = (startTime, endTime, period, name)

  override def toDetailedString: String = {
    s"Timer($name,s:$startTime,e:$endTime,p:$period)"
  }

}

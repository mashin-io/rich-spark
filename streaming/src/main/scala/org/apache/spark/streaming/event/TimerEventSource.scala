
package org.apache.spark.streaming.event

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.util.Clock

case class TimerEvent(source: EventSource, override val time: Long, index: Long)
  extends Event(source)

class TimerEventSource(
    ssc: StreamingContext,
    clock: Clock,
    startTime: Long,
    endTime: Long,
    period: Long,
    name: String
  ) extends EventSource(ssc) with Logging {

  require(startTime < endTime,
    s"End time ($endTime ms) should be greater than start time ($startTime ms).")

  require(period > 0, s"Period ($period ms) should be greater than 0.")

  private var stopped = false
  private var nextTime: Long = -1

  private val thread = new Thread(s"TimerEventSource $name") {
    setDaemon(true)
    override def run() { loop() }
  }

  private def loop(): Unit = {
    var index = 0
    while (!stopped && nextTime <= endTime) {
      clock.waitTillTime(nextTime)
      post(TimerEvent(this, nextTime, index))
      logDebug(s"Timer ($name) event with time $nextTime ms and period index $index")
      index += 1
      nextTime += period
    }
    stop()
  }

  override def start(): Unit = synchronized {
    val currTime = System.currentTimeMillis
    if (startTime < currTime) {
      // if start time already elapsed, start from the next possible
      // period after current time
      nextTime = startTime + period * (1 + (currTime - startTime) / period)
    } else {
      nextTime = startTime
    }
    thread.start()
    logDebug(s"Timer ($name) started, next time $nextTime ms")
  }

  override def stop(): Unit = synchronized {
    if (!stopped) {
      stopped = true
      logDebug(s"Timer ($name) stopped")
    }
  }
}

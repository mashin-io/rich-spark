
package org.apache.spark.streaming.event

import org.apache.spark.streaming.{Duration, StreamingContext, Time}

case class TimerEvent(source: EventSource, override val time: Time, index: Long)
  extends Event(source)

class TimerEventSource(
    @transient ssc: StreamingContext,
    private var startTime: Time,
    private var endTime: Time,
    private var period: Duration,
    name: String
  ) extends EventSource(ssc) {

  private var stopped = false
  private var nextTime: Time = Time(-1)

  def getStartTime: Time = startTime.copy()

  def getEndTime: Time = endTime.copy()

  def getPeriod: Duration = period.copy()

  def setStartTime(time: Time) {
    startTime = time
  }

  def setEndTime(time: Time) {
    endTime = time
  }

  def setPeriod(period: Duration) {
    this.period = period
  }

  override def start(): Unit = synchronized {
    require(startTime < endTime,
      s"End time ($endTime ms) should be greater than start time ($startTime ms).")

    require(period.milliseconds > 0, s"Period $period should be greater than 0.")

    val currTime = Time(System.currentTimeMillis)
    if (startTime < currTime) {
      // if start time already elapsed, start from the next possible
      // period after current time
      nextTime = startTime + period * (1 + (currTime - startTime) / period).toInt
    } else {
      nextTime = startTime
    }
    stopped = false
    new Thread(s"TimerEventSource $name") {
      setDaemon(true)
      override def run() { loop() }
    }.start()
    logDebug(s"Timer ($name) started, first timer event emits on $nextTime ms")
  }

  private def loop(): Unit = {
    val clock = context.clock
    var index = 0
    while (!stopped && nextTime <= endTime) {
      clock.waitTillTime(nextTime.milliseconds)
      post(TimerEvent(this, nextTime, index))
      logDebug(s"Timer ($name) event with time $nextTime ms and period index $index")
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
      .map(t => TimerEvent(this, t, ((t - startTime) / period).toInt))
  }

}

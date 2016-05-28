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

package org.apache.spark.streaming.scheduler

import org.apache.spark.streaming.event.{TimerEventSource, TimerEvent}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.streaming.{Seconds, Duration, StreamingContext, Time}

class InputInfoTrackerSuite extends SparkFunSuite with BeforeAndAfter {

  private var ssc: StreamingContext = _

  before {
    val conf = new SparkConf().setMaster("local[2]").setAppName("DirectStreamTacker")
    if (ssc == null) {
      ssc = new StreamingContext(conf, Duration(1000))
    }
  }

  after {
    if (ssc != null) {
      ssc.stop()
      ssc = null
    }
  }

  test("test report and get InputInfo from InputInfoTracker") {
    val inputInfoTracker = new InputInfoTracker(ssc)

    val streamId1 = 0
    val streamId2 = 1
    val timer = new TimerEventSource(null, Time(0), Time(1000), Seconds(1), "null-timer")
    val event = new TimerEvent(timer, Time(0L), 0)
    val inputInfo1 = StreamInputInfo(streamId1, 100L)
    val inputInfo2 = StreamInputInfo(streamId2, 300L)
    inputInfoTracker.reportInfo(event, inputInfo1)
    inputInfoTracker.reportInfo(event, inputInfo2)

    val batchEventToInputInfos = inputInfoTracker.getInfo(event)
    assert(batchEventToInputInfos.size == 2)
    assert(batchEventToInputInfos.keys === Set(streamId1, streamId2))
    assert(batchEventToInputInfos(streamId1) === inputInfo1)
    assert(batchEventToInputInfos(streamId2) === inputInfo2)
    assert(inputInfoTracker.getInfo(event)(streamId1) === inputInfo1)
  }

  test("test cleanup InputInfo from InputInfoTracker") {
    val inputInfoTracker = new InputInfoTracker(ssc)

    val streamId1 = 0
    val timer = new TimerEventSource(null, Time(0), Time(1000), Seconds(1), "null-timer")
    val event1 = new TimerEvent(timer, Time(0), 0)
    val event2 = new TimerEvent(timer, Time(1), 1)
    val inputInfo1 = StreamInputInfo(streamId1, 100L)
    val inputInfo2 = StreamInputInfo(streamId1, 300L)
    inputInfoTracker.reportInfo(event1, inputInfo1)
    inputInfoTracker.reportInfo(event2, inputInfo2)

    inputInfoTracker.cleanup(Time(0))
    assert(inputInfoTracker.getInfo(event1)(streamId1) === inputInfo1)
    assert(inputInfoTracker.getInfo(event2)(streamId1) === inputInfo2)

    inputInfoTracker.cleanup(Time(1))
    assert(inputInfoTracker.getInfo(event1).get(streamId1) === None)
    assert(inputInfoTracker.getInfo(event2)(streamId1) === inputInfo2)
  }
}

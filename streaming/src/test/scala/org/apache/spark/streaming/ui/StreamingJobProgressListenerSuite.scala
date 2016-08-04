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

package org.apache.spark.streaming.ui

import java.util.Properties

import org.apache.spark.streaming.event.{Event, TimerEventSource, TimerEvent}
import org.scalatest.Matchers

import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.scheduler._

class StreamingJobProgressListenerSuite extends TestSuiteBase with Matchers {

  val input = (1 to 4).map(Seq(_)).toSeq
  val operation = (d: DStream[Int]) => d.map(x => x)

  var ssc: StreamingContext = _

  override def afterFunction() {
    super.afterFunction()
    if (ssc != null) {
      ssc.stop()
    }
  }

  private def createJobStart(
      batchEvent: Event, outputOpId: Int, jobId: Int): SparkListenerJobStart = {
    val properties = new Properties()
    properties.setProperty(JobScheduler.BATCH_EVENT_PROPERTY_KEY, batchEvent.instanceId.toString)
    properties.setProperty(JobScheduler.OUTPUT_OP_ID_PROPERTY_KEY, outputOpId.toString)
    SparkListenerJobStart(jobId = jobId,
      0L, // unused
      Nil, // unused
      properties)
  }

  override def batchDuration: Duration = Milliseconds(100)

  val timer = new TimerEventSource(null, Time(0), Time(10000), Seconds(10), "null-timer")

  private def timerEvent(time: Time): TimerEvent = {
    new TimerEvent(timer, time.milliseconds / batchDuration.milliseconds - 1, time)
  }

  test("onBatchSubmitted, onBatchStarted, onBatchCompleted, " +
    "onReceiverStarted, onReceiverError, onReceiverStopped") {
    ssc = setupStreams(input, operation)
    val listener = new StreamingJobProgressListener(ssc)
    val event = timerEvent(Time(1000))

    val streamIdToInputInfo = Map(
      0 -> StreamInputInfo(0, 300L),
      1 -> StreamInputInfo(1, 300L, Map(StreamInputInfo.METADATA_KEY_DESCRIPTION -> "test")))

    // onBatchSubmitted
    val batchInfoSubmitted = BatchInfo(event, streamIdToInputInfo, 1000, None, None, Map.empty)
    listener.onBatchSubmitted(StreamingListenerBatchSubmitted(batchInfoSubmitted))
    listener.waitingBatches should be (List(BatchUIData(batchInfoSubmitted)))
    listener.runningBatches should be (Nil)
    listener.retainedCompletedBatches should be (Nil)
    listener.lastCompletedBatch should be (None)
    listener.numUnprocessedBatches should be (1)
    listener.numTotalCompletedBatches should be (0)
    listener.numTotalProcessedRecords should be (0)
    listener.numTotalReceivedRecords should be (0)

    // onBatchStarted
    val batchInfoStarted =
      BatchInfo(event, streamIdToInputInfo, 1000, Some(2000), None, Map.empty)
    listener.onBatchStarted(StreamingListenerBatchStarted(batchInfoStarted))
    listener.waitingBatches should be (Nil)
    listener.runningBatches should be (List(BatchUIData(batchInfoStarted)))
    listener.retainedCompletedBatches should be (Nil)
    listener.lastCompletedBatch should be (None)
    listener.numUnprocessedBatches should be (1)
    listener.numTotalCompletedBatches should be (0)
    listener.numTotalProcessedRecords should be (0)
    listener.numTotalReceivedRecords should be (600)

    // onJobStart
    val jobStart1 = createJobStart(event, outputOpId = 0, jobId = 0)
    listener.onJobStart(jobStart1)

    val jobStart2 = createJobStart(event, outputOpId = 0, jobId = 1)
    listener.onJobStart(jobStart2)

    val jobStart3 = createJobStart(event, outputOpId = 1, jobId = 0)
    listener.onJobStart(jobStart3)

    val jobStart4 = createJobStart(event, outputOpId = 1, jobId = 1)
    listener.onJobStart(jobStart4)

    val batchUIData = listener.getBatchUIData(event.instanceId)
    batchUIData should not be None
    batchUIData.get.batchEvent should be (batchInfoStarted.batchEvent)
    batchUIData.get.schedulingDelay should be (batchInfoStarted.schedulingDelay)
    batchUIData.get.processingDelay should be (batchInfoStarted.processingDelay)
    batchUIData.get.totalDelay should be (batchInfoStarted.totalDelay)
    batchUIData.get.streamIdToInputInfo should be (Map(
      0 -> StreamInputInfo(0, 300L),
      1 -> StreamInputInfo(1, 300L, Map(StreamInputInfo.METADATA_KEY_DESCRIPTION -> "test"))))
    batchUIData.get.numRecords should be(600)
    batchUIData.get.outputOpIdSparkJobIdPairs should be
      Seq(OutputOpIdAndSparkJobId(0, 0),
        OutputOpIdAndSparkJobId(0, 1),
        OutputOpIdAndSparkJobId(1, 0),
        OutputOpIdAndSparkJobId(1, 1))

    // onBatchCompleted
    val batchInfoCompleted =
      BatchInfo(event, streamIdToInputInfo, 1000, Some(2000), None, Map.empty)
    listener.onBatchCompleted(StreamingListenerBatchCompleted(batchInfoCompleted))
    listener.waitingBatches should be (Nil)
    listener.runningBatches should be (Nil)
    listener.retainedCompletedBatches should be (List(BatchUIData(batchInfoCompleted)))
    listener.lastCompletedBatch should be (Some(BatchUIData(batchInfoCompleted)))
    listener.numUnprocessedBatches should be (0)
    listener.numTotalCompletedBatches should be (1)
    listener.numTotalProcessedRecords should be (600)
    listener.numTotalReceivedRecords should be (600)

    // onReceiverStarted
    val receiverInfoStarted = ReceiverInfo(0, "test", true, "localhost", "0")
    listener.onReceiverStarted(StreamingListenerReceiverStarted(receiverInfoStarted))
    listener.receiverInfo(0) should be (Some(receiverInfoStarted))
    listener.receiverInfo(1) should be (None)

    // onReceiverError
    val receiverInfoError = ReceiverInfo(1, "test", true, "localhost", "1")
    listener.onReceiverError(StreamingListenerReceiverError(receiverInfoError))
    listener.receiverInfo(0) should be (Some(receiverInfoStarted))
    listener.receiverInfo(1) should be (Some(receiverInfoError))
    listener.receiverInfo(2) should be (None)

    // onReceiverStopped
    val receiverInfoStopped = ReceiverInfo(2, "test", true, "localhost", "2")
    listener.onReceiverStopped(StreamingListenerReceiverStopped(receiverInfoStopped))
    listener.receiverInfo(0) should be (Some(receiverInfoStarted))
    listener.receiverInfo(1) should be (Some(receiverInfoError))
    listener.receiverInfo(2) should be (Some(receiverInfoStopped))
    listener.receiverInfo(3) should be (None)
  }

  test("Remove the old completed batches when exceeding the limit") {
    ssc = setupStreams(input, operation)
    val limit = ssc.conf.getInt("spark.streaming.ui.retainedBatches", 1000)
    val listener = new StreamingJobProgressListener(ssc)
    val event = timerEvent(Time(1000))

    val streamIdToInputInfo = Map(0 -> StreamInputInfo(0, 300L), 1 -> StreamInputInfo(1, 300L))

    val batchInfoCompleted =
      BatchInfo(event, streamIdToInputInfo, 1000, Some(2000), None, Map.empty)

    for(_ <- 0 until (limit + 10)) {
      listener.onBatchCompleted(StreamingListenerBatchCompleted(batchInfoCompleted))
    }

    listener.retainedCompletedBatches.size should be (limit)
    listener.numTotalCompletedBatches should be(limit + 10)
  }

  test("out-of-order onJobStart and onBatchXXX") {
    ssc = setupStreams(input, operation)
    val limit = ssc.conf.getInt("spark.streaming.ui.retainedBatches", 1000)
    val listener = new StreamingJobProgressListener(ssc)
    val events = (0 until limit).map(i => timerEvent(Time(1000 + i * 100)))
    val eventOnLimit = timerEvent(Time(1000 + limit * 100))

    // fulfill completedBatchInfos
    for(i <- 0 until limit) {
      val batchInfoCompleted = BatchInfo(
        events(i), Map.empty, 1000 + i * 100, Some(2000 + i * 100), None, Map.empty)
      listener.onBatchCompleted(StreamingListenerBatchCompleted(batchInfoCompleted))
      val jobStart = createJobStart(events(i), outputOpId = 0, jobId = 1)
      listener.onJobStart(jobStart)
    }

    // onJobStart happens before onBatchSubmitted
    val jobStart = createJobStart(eventOnLimit, outputOpId = 0, jobId = 0)
    listener.onJobStart(jobStart)

    val batchInfoSubmitted =
      BatchInfo(eventOnLimit, Map.empty, (1000 + limit * 100), None, None, Map.empty)
    listener.onBatchSubmitted(StreamingListenerBatchSubmitted(batchInfoSubmitted))

    // We still can see the info retrieved from onJobStart
    val batchUIDataOption = listener.getBatchUIData(eventOnLimit.instanceId)
    batchUIDataOption should not be None
    val batchUIData = batchUIDataOption.get
    batchUIData.batchEvent should be (batchInfoSubmitted.batchEvent)
    batchUIData.schedulingDelay should be (batchInfoSubmitted.schedulingDelay)
    batchUIData.processingDelay should be (batchInfoSubmitted.processingDelay)
    batchUIData.totalDelay should be (batchInfoSubmitted.totalDelay)
    batchUIData.streamIdToInputInfo should be (Map.empty)
    batchUIData.numRecords should be (0)
    batchUIData.outputOpIdSparkJobIdPairs.toSeq should be (Seq(OutputOpIdAndSparkJobId(0, 0)))

    val eventsAfterLimit = (limit + 1 to limit * 2).map(i => timerEvent(Time(1000 + i * 100)))

    // A lot of "onBatchCompleted"s happen before "onJobStart"
    for(i <- limit + 1 to limit * 2) {
      val batchInfoCompleted = BatchInfo(
        eventsAfterLimit(i - limit - 1), Map.empty,
        1000 + i * 100, Some(2000 + i * 100), None, Map.empty)
      listener.onBatchCompleted(StreamingListenerBatchCompleted(batchInfoCompleted))
    }

    for(i <- limit + 1 to limit * 2) {
      val jobStart = createJobStart(eventsAfterLimit(i - limit - 1), outputOpId = 0, jobId = 1)
      listener.onJobStart(jobStart)
    }

    // We should not leak memory
    listener.batchEventToOutputOpIdSparkJobIdPair.size() should be <=
      (listener.waitingBatches.size + listener.runningBatches.size +
        listener.retainedCompletedBatches.size + 10)
  }

  test("detect memory leak") {
    ssc = setupStreams(input, operation)
    val listener = new StreamingJobProgressListener(ssc)
    val event = timerEvent(Time(1000))

    val limit = ssc.conf.getInt("spark.streaming.ui.retainedBatches", 1000)

    for (_ <- 0 until 2 * limit) {
      val streamIdToInputInfo = Map(0 -> StreamInputInfo(0, 300L), 1 -> StreamInputInfo(1, 300L))

      // onBatchSubmitted
      val batchInfoSubmitted =
        BatchInfo(event, streamIdToInputInfo, 1000, None, None, Map.empty)
      listener.onBatchSubmitted(StreamingListenerBatchSubmitted(batchInfoSubmitted))

      // onBatchStarted
      val batchInfoStarted =
        BatchInfo(event, streamIdToInputInfo, 1000, Some(2000), None, Map.empty)
      listener.onBatchStarted(StreamingListenerBatchStarted(batchInfoStarted))

      // onJobStart
      val jobStart1 = createJobStart(event, outputOpId = 0, jobId = 0)
      listener.onJobStart(jobStart1)

      val jobStart2 = createJobStart(event, outputOpId = 0, jobId = 1)
      listener.onJobStart(jobStart2)

      val jobStart3 = createJobStart(event, outputOpId = 1, jobId = 0)
      listener.onJobStart(jobStart3)

      val jobStart4 = createJobStart(event, outputOpId = 1, jobId = 1)
      listener.onJobStart(jobStart4)

      // onBatchCompleted
      val batchInfoCompleted =
        BatchInfo(event, streamIdToInputInfo, 1000, Some(2000), None, Map.empty)
      listener.onBatchCompleted(StreamingListenerBatchCompleted(batchInfoCompleted))
    }

    listener.waitingBatches.size should be (0)
    listener.runningBatches.size should be (0)
    listener.retainedCompletedBatches.size should be (limit)
    listener.batchEventToOutputOpIdSparkJobIdPair.size() should be <=
      (listener.waitingBatches.size + listener.runningBatches.size +
        listener.retainedCompletedBatches.size + 10)
  }

}

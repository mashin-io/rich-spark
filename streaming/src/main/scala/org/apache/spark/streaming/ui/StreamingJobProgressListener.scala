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

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.{LinkedHashMap, Map => JMap, Properties}

import org.apache.spark.scheduler._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.event.Event
import org.apache.spark.streaming.scheduler._

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, Queue}

private[streaming] class StreamingJobProgressListener(ssc: StreamingContext)
  extends SparkListener with StreamingListener {

  private val waitingBatchUIData = new HashMap[Long, BatchUIData]
  private val runningBatchUIData = new HashMap[Long, BatchUIData]
  private val completedBatchUIData = new Queue[BatchUIData]
  private val batchUIDataLimit = ssc.conf.getInt("spark.streaming.ui.retainedBatches", 1000)
  private var totalCompletedBatches = 0L
  private var totalReceivedRecords = 0L
  private var totalProcessedRecords = 0L
  private val receiverInfos = new HashMap[Int, ReceiverInfo]

  // Because onJobStart and onBatchXXX messages are processed in different threads,
  // we may not be able to get the corresponding BatchUIData when receiving onJobStart. So here we
  // cannot use a map of (Long, BatchUIData).
  private[ui] val batchEventToOutputOpIdSparkJobIdPair =
    new LinkedHashMap[Long, ConcurrentLinkedQueue[OutputOpIdAndSparkJobId]] {
      override def removeEldestEntry(
          p1: JMap.Entry[Long, ConcurrentLinkedQueue[OutputOpIdAndSparkJobId]]): Boolean = {
        // If a lot of "onBatchCompleted"s happen before "onJobStart" (image if
        // SparkContext.listenerBus is very slow), "batchTimeToOutputOpIdToSparkJobIds"
        // may add some information for a removed batch when processing "onJobStart". It will be a
        // memory leak.
        //
        // To avoid the memory leak, we control the size of "batchTimeToOutputOpIdToSparkJobIds" and
        // evict the eldest one.
        //
        // Note: if "onJobStart" happens before "onBatchSubmitted", the size of
        // "batchTimeToOutputOpIdToSparkJobIds" may be greater than the number of the retained
        // batches temporarily, so here we use "10" to handle such case. This is not a perfect
        // solution, but at least it can handle most of cases.
        size() >
          waitingBatchUIData.size + runningBatchUIData.size + completedBatchUIData.size + 10
      }
    }


  val batchDuration = ssc.graph.batchDuration.milliseconds

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) {
    synchronized {
      receiverInfos(receiverStarted.receiverInfo.streamId) = receiverStarted.receiverInfo
    }
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError) {
    synchronized {
      receiverInfos(receiverError.receiverInfo.streamId) = receiverError.receiverInfo
    }
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped) {
    synchronized {
      receiverInfos(receiverStopped.receiverInfo.streamId) = receiverStopped.receiverInfo
    }
  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
    synchronized {
      waitingBatchUIData(batchSubmitted.batchInfo.batchEvent.instanceId) =
        BatchUIData(batchSubmitted.batchInfo)
    }
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = synchronized {
    val batchUIData = BatchUIData(batchStarted.batchInfo)
    runningBatchUIData(batchStarted.batchInfo.batchEvent.instanceId) = batchUIData
    waitingBatchUIData.remove(batchStarted.batchInfo.batchEvent.instanceId)

    totalReceivedRecords += batchUIData.numRecords
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    synchronized {
      waitingBatchUIData.remove(batchCompleted.batchInfo.batchEvent.instanceId)
      runningBatchUIData.remove(batchCompleted.batchInfo.batchEvent.instanceId)
      val batchUIData = BatchUIData(batchCompleted.batchInfo)
      completedBatchUIData.enqueue(batchUIData)
      if (completedBatchUIData.size > batchUIDataLimit) {
        val removedBatch = completedBatchUIData.dequeue()
        batchEventToOutputOpIdSparkJobIdPair.remove(removedBatch.batchEvent.instanceId)
      }
      totalCompletedBatches += 1L

      totalProcessedRecords += batchUIData.numRecords
    }
  }

  override def onOutputOperationStarted(
      outputOperationStarted: StreamingListenerOutputOperationStarted): Unit = synchronized {
    // This method is called after onBatchStarted
    runningBatchUIData(outputOperationStarted.outputOperationInfo.batchEvent.instanceId).
      updateOutputOperationInfo(outputOperationStarted.outputOperationInfo)
  }

  override def onOutputOperationCompleted(
      outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = synchronized {
    // This method is called before onBatchCompleted
    runningBatchUIData(outputOperationCompleted.outputOperationInfo.batchEvent.instanceId).
      updateOutputOperationInfo(outputOperationCompleted.outputOperationInfo)
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
    getBatchEventIdAndOutputOpId(jobStart.properties).foreach { case (batchEventId, outputOpId) =>
      var outputOpIdToSparkJobIds = batchEventToOutputOpIdSparkJobIdPair.get(batchEventId)
      if (outputOpIdToSparkJobIds == null) {
        outputOpIdToSparkJobIds = new ConcurrentLinkedQueue[OutputOpIdAndSparkJobId]()
        batchEventToOutputOpIdSparkJobIdPair.put(batchEventId, outputOpIdToSparkJobIds)
      }
      outputOpIdToSparkJobIds.add(OutputOpIdAndSparkJobId(outputOpId, jobStart.jobId))
    }
  }

  private def getBatchEventIdAndOutputOpId(properties: Properties): Option[(Long, Int)] = {
    val batchEventId = properties.getProperty(JobScheduler.BATCH_EVENT_PROPERTY_KEY)
    if (batchEventId == null) {
      // Not submitted from JobScheduler
      None
    } else {
      val outputOpId = properties.getProperty(JobScheduler.OUTPUT_OP_ID_PROPERTY_KEY)
      assert(outputOpId != null)
      Some(batchEventId.toLong -> outputOpId.toInt)
    }
  }

  def numReceivers: Int = synchronized {
    receiverInfos.size
  }

  def numActiveReceivers: Int = synchronized {
    receiverInfos.count(_._2.active)
  }

  def numInactiveReceivers: Int = {
    ssc.graph.getReceiverInputStreams().length - numActiveReceivers
  }

  def numTotalCompletedBatches: Long = synchronized {
    totalCompletedBatches
  }

  def numTotalReceivedRecords: Long = synchronized {
    totalReceivedRecords
  }

  def numTotalProcessedRecords: Long = synchronized {
    totalProcessedRecords
  }

  def numUnprocessedBatches: Long = synchronized {
    waitingBatchUIData.size + runningBatchUIData.size
  }

  def waitingBatches: Seq[BatchUIData] = synchronized {
    waitingBatchUIData.values.toSeq
  }

  def runningBatches: Seq[BatchUIData] = synchronized {
    runningBatchUIData.values.toSeq
  }

  def retainedCompletedBatches: Seq[BatchUIData] = synchronized {
    completedBatchUIData.toSeq
  }

  def streamName(streamId: Int): Option[String] = {
    ssc.graph.getInputStreamName(streamId)
  }

  /**
   * Return all InputDStream Ids
   */
  def streamIds: Seq[Int] = ssc.graph.getInputStreams().map(_.id)

  /**
   * Return all of the record rates for each InputDStream in each batch. The key of the return value
   * is the stream id, and the value is a sequence of batch time with its record rate.
   */
  def receivedRecordRateWithBatchTime: Map[Int, Seq[(Long, Double)]] = synchronized {
    val _retainedBatches = retainedBatches
    val latestBatches = _retainedBatches.map { batchUIData =>
      (batchUIData.batchEvent.time.milliseconds, batchUIData.streamIdToInputInfo.mapValues(_.numRecords))
    }
    streamIds.map { streamId =>
      val recordRates = latestBatches.map {
        case (batchTime, streamIdToNumRecords) =>
          val numRecords = streamIdToNumRecords.getOrElse(streamId, 0L)
          (batchTime, numRecords * 1000.0 / batchDuration)
      }
      (streamId, recordRates)
    }.toMap
  }

  def lastReceivedBatchRecords: Map[Int, Long] = synchronized {
    val lastReceivedBlockInfoOption =
      lastReceivedBatch.map(_.streamIdToInputInfo.mapValues(_.numRecords))
    lastReceivedBlockInfoOption.map { lastReceivedBlockInfo =>
      streamIds.map { streamId =>
        (streamId, lastReceivedBlockInfo.getOrElse(streamId, 0L))
      }.toMap
    }.getOrElse {
      streamIds.map(streamId => (streamId, 0L)).toMap
    }
  }

  def receiverInfo(receiverId: Int): Option[ReceiverInfo] = synchronized {
    receiverInfos.get(receiverId)
  }

  def lastCompletedBatch: Option[BatchUIData] = synchronized {
    completedBatchUIData.sortBy(_.batchEvent)(Event.ordering).lastOption
  }

  def lastReceivedBatch: Option[BatchUIData] = synchronized {
    retainedBatches.lastOption
  }

  def retainedBatches: Seq[BatchUIData] = synchronized {
    (waitingBatchUIData.values.toSeq ++
      runningBatchUIData.values.toSeq ++ completedBatchUIData).sortBy(_.batchEvent)(Event.ordering)
  }

  def getBatchUIData(batchEventInstanceId: Long): Option[BatchUIData] = synchronized {
    val batchUIData = waitingBatchUIData.get(batchEventInstanceId).orElse {
      runningBatchUIData.get(batchEventInstanceId).orElse {
        completedBatchUIData.find(batch => batch.batchEvent.instanceId == batchEventInstanceId)
      }
    }
    batchUIData.foreach { _batchUIData =>
      // We use an Iterable rather than explicitly converting to a seq so that updates
      // will propagate
      val outputOpIdToSparkJobIds: Iterable[OutputOpIdAndSparkJobId] =
        Option(batchEventToOutputOpIdSparkJobIdPair.get(batchEventInstanceId))
          .map(_.asScala).getOrElse(Seq.empty)
      _batchUIData.outputOpIdSparkJobIdPairs = outputOpIdToSparkJobIds
    }
    batchUIData
  }
}

private[streaming] object StreamingJobProgressListener {
  type SparkJobId = Int
  type OutputOpId = Int
}

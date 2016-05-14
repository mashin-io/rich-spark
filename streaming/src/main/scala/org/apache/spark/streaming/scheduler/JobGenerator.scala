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

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.event._
import org.apache.spark.streaming.{Checkpoint, CheckpointWriter, Time}
import org.apache.spark.util.{EventLoop, ManualClock}

import scala.util.{Failure, Success, Try}

/** Event classes for JobGenerator */
private[scheduler] sealed trait JobGeneratorEvent
private[scheduler] case class GenerateJobs(event: Event) extends JobGeneratorEvent
private[scheduler] case class ClearMetadata(event: Event) extends JobGeneratorEvent
private[scheduler] case class DoCheckpoint(
    event: Event, clearCheckpointDataLater: Boolean) extends JobGeneratorEvent
private[scheduler] case class ClearCheckpointData(event: Event) extends JobGeneratorEvent

/**
 * This class generates jobs from DStreams as well as drives checkpointing and cleaning
 * up DStream metadata.
 */
private[streaming]
class JobGenerator(jobScheduler: JobScheduler) extends Logging {

  private val ssc = jobScheduler.ssc
  private val conf = ssc.conf
  private val graph = ssc.graph
  private val clock = ssc.clock

  val eventBus: EventListenerBus = new EventListenerBus

  // This is marked lazy so that this is initialized after checkpoint duration has been set
  // in the context and the generator has been started.
  private lazy val shouldCheckpoint = ssc.checkpointDuration != null && ssc.checkpointDir != null

  private lazy val checkpointWriter = if (shouldCheckpoint) {
    new CheckpointWriter(this, ssc.conf, ssc.checkpointDir, ssc.sparkContext.hadoopConfiguration)
  } else {
    null
  }

  // eventLoop is created when generator starts.
  // This not being null means the scheduler has been started and not stopped
  private var eventLoop: EventLoop[JobGeneratorEvent] = null
  private val remainingEventsCount = new AtomicLong(0)

  // last batch whose completion,checkpointing and metadata cleanup has been completed
  private var lastProcessedBatch: Time = null

  /** Start generation of jobs */
  def start(): Unit = synchronized {
    if (eventLoop != null) return // generator has already been started

    // Call checkpointWriter here to initialize it before eventLoop uses it to avoid a deadlock.
    // See SPARK-10125
    checkpointWriter

    eventLoop = new EventLoop[JobGeneratorEvent]("JobGenerator") {
      override protected def onReceive(event: JobGeneratorEvent): Unit = processEvent(event)

      override protected def onError(e: Throwable): Unit = {
        jobScheduler.reportError("Error in job generator", e)
      }
    }
    eventLoop.start()

    eventBus.addListener(new EventListener {
      override def onEvent(event: Event) = {
        eventLoop.post(GenerateJobs(event))
      }
    })

    if (ssc.isCheckpointPresent) {
      restart()
    } else {
      startFirstTime()
    }
  }

  /**
   * Stop generation of jobs. processReceivedData = true makes this wait until jobs
   * of current ongoing event interval has been generated, processed and corresponding
   * checkpoints written.
   */
  def stop(processReceivedData: Boolean): Unit = synchronized {
    if (eventLoop == null) return // generator has already been stopped

    if (processReceivedData) {
      logInfo("Stopping JobGenerator gracefully")
      val timeWhenStopStarted = System.currentTimeMillis()
      val stopTimeoutMs = conf.getTimeAsMs(
        "spark.streaming.gracefulStopTimeout", s"${10 * ssc.graph.batchDuration.milliseconds}ms")
      val pollTime = 100

      // To prevent graceful stop to get stuck permanently
      def hasTimedOut: Boolean = {
        val timedOut = (System.currentTimeMillis() - timeWhenStopStarted) > stopTimeoutMs
        if (timedOut) {
          logWarning(s"Timed out while stopping the job generator (timeout = $stopTimeoutMs)")
        }
        timedOut
      }

      // Wait until all the received blocks in the network input tracker has
      // been consumed by network input DStreams, and jobs have been generated with them
      logInfo("Waiting for all received blocks to be consumed for job generation")
      while(!hasTimedOut && jobScheduler.receiverTracker.hasUnallocatedBlocks) {
        Thread.sleep(pollTime)
      }
      logInfo("Waited for all received blocks to be consumed for job generation")

      // Stop graph
      graph.stop()
      //val stopTime = clock.getTimeMillis()
      logInfo("Stopped graph")

      // Wait for the jobs to complete and checkpoints to be written
      def haveAllBatchesBeenProcessed: Boolean = {
        //lastProcessedBatch != null && lastProcessedBatch.milliseconds == stopTime
        remainingEventsCount.get == 0
      }
      logInfo("Waiting for jobs to be processed and checkpoints to be written")
      while (!hasTimedOut && !haveAllBatchesBeenProcessed) {
        Thread.sleep(pollTime)
      }
      logInfo("Waited for jobs to be processed and checkpoints to be written")
    } else {
      logInfo("Stopping JobGenerator immediately")
      // Stop graph immediately, ignore unprocessed data and pending jobs
      graph.stop()
    }

    // First stop the event loop, then stop the checkpoint writer; see SPARK-14701
    eventLoop.stop()
    if (shouldCheckpoint) checkpointWriter.stop()
    logInfo("Stopped JobGenerator")
  }

  /**
   * Callback called when a batch has been completely processed.
   */
  def onBatchCompletion(event: Event) {
    eventLoop.post(ClearMetadata(event))
  }

  /**
   * Callback called when the checkpoint of a batch has been written.
   */
  def onCheckpointCompletion(event: Event, clearCheckpointDataLater: Boolean) {
    if (clearCheckpointDataLater) {
      eventLoop.post(ClearCheckpointData(event))
    }
  }

  /** Processes all events */
  private def processEvent(jobGeneratorEvent: JobGeneratorEvent) {
    logDebug("Got event " + jobGeneratorEvent)
    jobGeneratorEvent match {
      case GenerateJobs(event) => generateJobs(event)
      case ClearMetadata(time) => clearMetadata(time)
      case DoCheckpoint(time, clearCheckpointDataLater) =>
        doCheckpoint(time, clearCheckpointDataLater)
      case ClearCheckpointData(time) => clearCheckpointData(time)
    }
  }

  /** Starts the generator for the first event */
  private def startFirstTime() {
    val startTime = Time(clock.getTimeMillis())
    graph.start(startTime - graph.batchDuration)
    logInfo("Started JobGenerator at " + startTime)
  }

  /** Restarts the generator based on the information in checkpoint */
  private def restart() {
    // If manual clock is being used for testing, then
    // either set the manual clock to the last checkpointed event,
    // or if the property is defined set it to that event
    clock match {
      case manualClock: ManualClock =>
        val lastTime = ssc.initialCheckpoint.checkpointEvent.time.milliseconds
        val jumpTime = ssc.sc.conf.getLong("spark.streaming.manualClock.jump", 0)
        manualClock.setTime(lastTime + jumpTime)
      case _ =>
    }

    // Batches when the master was down, that is,
    // between the checkpoint and current restart event
    val checkpointTime = ssc.initialCheckpoint.checkpointEvent.time
    val restartTime = Time(clock.getTimeMillis())
    val downTimeEvents = graph.eventSources.toSeq
      .flatMap(_.between(checkpointTime, restartTime))
      .sorted(Event.ordering)
    logInfo("Batches during down time (" + downTimeEvents.size + " batches): ("
      + downTimeEvents.mkString("), (") + ")")

    // Batches that were unprocessed before failure
    val pendingEvents = ssc.initialCheckpoint.pendingEvents.sorted(Event.ordering)
    logInfo("Batches pending processing (" + pendingEvents.length + " batches): (" +
      pendingEvents.mkString("), (") + ")")

    // Reschedule jobs for these times
    val eventsToReschedule = (pendingEvents ++ downTimeEvents)
      .filter { _.time < restartTime }
      .distinct.sorted(Event.ordering)
    logInfo("Batches to reschedule (" + eventsToReschedule.length + " batches): (" +
      eventsToReschedule.mkString("), (") + ")")

    eventsToReschedule.foreach { event =>
      // Allocate the related blocks when recovering from failure, because some blocks that were
      // added but not allocated, are dangling in the queue after recovering, we have to allocate
      // those blocks to the next batch, which is the batch they were supposed to go.
      graph.getInputStreams().filter(_.boundEventSources.contains(event.eventSource)).map(_.id)
        // allocate received blocks to batch
        .foreach(jobScheduler.receiverTracker.allocateBlocksToBatchAndStream(event, _))

      jobScheduler.submitJobSet(JobSet(event, graph.generateJobs(event)))
    }

    // Restart the graph
    graph.restart(restartTime)
    logInfo("Restarted JobGenerator at " + restartTime)
  }

  /** Generate jobs and perform checkpoint for the given `event`.  */
  private def generateJobs(event: Event) {
    remainingEventsCount.incrementAndGet()
    // Checkpoint all RDDs marked for checkpointing to ensure their lineages are
    // truncated periodically. Otherwise, we may run into stack overflows (SPARK-6847).
    ssc.sparkContext.setLocalProperty(RDD.CHECKPOINT_ALL_MARKED_ANCESTORS, "true")
    Try {
      //TODO: This is now done by each receiver input dstream generateJob method
      //// allocate received blocks to batch
      //graph.getBoundStreams(event.eventSource)
      //  .filter(_.isInstanceOf[ReceiverInputDStream[_]])
      //  .map(_.asInstanceOf[ReceiverInputDStream[_]].id)
      //  .foreach(jobScheduler.receiverTracker.allocateBlocksToBatch(event, _))
      graph.generateJobs(event) // generate jobs using allocated block
    } match {
      case Success(jobs) =>
        val streamIdToInputInfos = jobScheduler.inputInfoTracker.getInfo(event)
        jobScheduler.submitJobSet(JobSet(event, jobs, streamIdToInputInfos))
      case Failure(e) =>
        jobScheduler.reportError(s"Error generating jobs for event $event", e)
    }
    eventLoop.post(DoCheckpoint(event, clearCheckpointDataLater = false))
  }

  /** Clear DStream metadata for the given `event`. */
  private def clearMetadata(event: Event) {
    ssc.graph.clearMetadata(event)

    // If checkpointing is enabled, then checkpoint,
    // else mark batch to be fully processed
    if (shouldCheckpoint) {
      eventLoop.post(DoCheckpoint(event, clearCheckpointDataLater = true))
    } else {
      // If checkpointing is not enabled, then delete metadata information about
      // received blocks (block data not saved in any case). Otherwise, wait for
      // checkpointing of this batch to complete.
      val maxRememberDuration = graph.getMaxInputStreamRememberDuration()
      jobScheduler.receiverTracker.cleanupOldBlocksAndBatches(event.time - maxRememberDuration)
      jobScheduler.inputInfoTracker.cleanup(event.time - maxRememberDuration)
      markBatchFullyProcessed(event)
    }
  }

  /** Clear DStream checkpoint data for the given `event`. */
  private def clearCheckpointData(event: Event) {
    ssc.graph.clearCheckpointData(event)

    // All the checkpoint information about which batches have been processed, etc have
    // been saved to checkpoints, so its safe to delete block metadata and data WAL files
    val maxRememberDuration = graph.getMaxInputStreamRememberDuration()
    jobScheduler.receiverTracker.cleanupOldBlocksAndBatches(event.time - maxRememberDuration)
    jobScheduler.inputInfoTracker.cleanup(event.time - maxRememberDuration)
    markBatchFullyProcessed(event)
  }

  /** Perform checkpoint for the give `event`. */
  private def doCheckpoint(event: Event, clearCheckpointDataLater: Boolean) {
    if (shouldCheckpoint && (event.time - graph.zeroTime).isMultipleOf(ssc.checkpointDuration)) {
      logInfo("Checkpointing graph for event " + event)
      ssc.graph.updateCheckpointData(event)
      checkpointWriter.write(new Checkpoint(ssc, event), clearCheckpointDataLater)
    } else if (clearCheckpointDataLater) {
      //TODO: validate this condition and raise an issue
      markBatchFullyProcessed(event)
    }
  }

  private def markBatchFullyProcessed(event: Event) {
    lastProcessedBatch = event.time
    remainingEventsCount.decrementAndGet()
  }
}

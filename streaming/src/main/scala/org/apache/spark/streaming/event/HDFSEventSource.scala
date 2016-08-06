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

import java.net.URI
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConversions
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.fs.XAttr.NameSpace
import org.apache.hadoop.fs.{XAttr, GlobPattern}
import org.apache.hadoop.fs.permission._
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream
import org.apache.hadoop.hdfs.client.HdfsAdmin
import org.apache.hadoop.hdfs.inotify.Event.CreateEvent.INodeType
import org.apache.hadoop.hdfs.inotify.Event.MetadataUpdateEvent.MetadataType
import org.apache.hadoop.hdfs.inotify.Event._
import org.apache.hadoop.hdfs.inotify.{Event => INotifyEvent}

import org.apache.spark.SparkException
import org.apache.spark.streaming.{StreamingContext, Time}

abstract class HDFSEvent(source: EventSource, index: Long, time: Time)
  extends Event(source, index, time) {

  def getEventType: EventType

  def toINotifyEvent: INotifyEvent

}

private[streaming] object HDFSEvent {
  def apply(
      source: EventSource,
      iNotifyEvent: INotifyEvent,
      index: Long,
      time: Time): HDFSEvent = iNotifyEvent match {
    case e: CloseEvent =>
      val t = e.getTimestamp
      HDFSCloseEvent(source, e.getPath, e.getFileSize, t, index, Time(t))

    case e: CreateEvent =>
      val t = e.getCtime
      val perms = e.getPerms
      HDFSCreateEvent(source, e.getiNodeType, e.getPath, t,
        e.getReplication, e.getOwnerName, e.getGroupName, HFsPermission(
          perms.getUserAction, perms.getGroupAction, perms.getOtherAction, perms.getStickyBit),
        e.getSymlinkTarget, e.getOverwrite, e.getDefaultBlockSize, index, Time(t))

    case e: MetadataUpdateEvent =>
      val at = e.getAtime
      val mt = e.getMtime
      val perms = e.getPerms
      HDFSMetadataUpdateEvent(source, e.getPath, e.getMetadataType, mt,
        at, e.getReplication, e.getOwnerName, e.getGroupName, HFsPermission(
          perms.getUserAction, perms.getGroupAction, perms.getOtherAction, perms.getStickyBit),
        JavaConversions.collectionAsScalaIterable(e.getAcls).map { aclEntry =>
          HAclEntry(aclEntry.getType, aclEntry.getName, aclEntry.getPermission, aclEntry.getScope)
        }.toList,
        JavaConversions.collectionAsScalaIterable(e.getxAttrs).map { xAttr =>
          HXAttr(xAttr.getNameSpace, xAttr.getName, xAttr.getValue)
        }.toList,
        e.isxAttrsRemoved, index, Time(at max mt)
      )

    case e: RenameEvent =>
      val t = e.getTimestamp
      HDFSRenameEvent(source, e.getSrcPath, e.getDstPath, t, index, Time(t))

    case e: AppendEvent =>
      HDFSAppendEvent(source, e.getPath, e.toNewBlock, index, time)

    case e: UnlinkEvent =>
      val t = e.getTimestamp
      HDFSUnlinkEvent(source, e.getPath, t, index, Time(t))
  }
}

case class HFsPermission(
    userAction: FsAction,
    groupAction: FsAction,
    otherAction: FsAction,
    stickyBit: Boolean) {
  def toFsPermission: FsPermission =
    new FsPermission(userAction, groupAction, otherAction, stickyBit)
}

case class HAclEntry(
    `type`: AclEntryType,
    name: String,
    permission: FsAction,
    scope: AclEntryScope
  ) {
  def toAclEntry: AclEntry = new AclEntry.Builder()
    .setType(`type`)
    .setName(name)
    .setPermission(permission)
    .setScope(scope)
    .build()
}

case class HXAttr(ns: NameSpace, name: String, value: Array[Byte]) {
  def toXAttr: XAttr = new XAttr.Builder()
    .setNameSpace(ns)
    .setName(name)
    .setValue(value)
    .build()
}

/** Sent when a file is closed after append or create. */
case class HDFSCloseEvent(
    source: EventSource,
    path: String,
    fileSize: Long,
    timestamp: Long,
    override val index: Long,
    override val time: Time
  ) extends HDFSEvent(source, index, time) {

  override def getEventType: EventType = EventType.CLOSE

  override def toINotifyEvent: CloseEvent = {
    new CloseEvent(path, fileSize, timestamp)
  }

}

/** Sent when a new file is created (including overwrite). */
case class HDFSCreateEvent(
    source: EventSource,
    iNodeType: INodeType,
    path: String,
    ctime: Long,
    replication: Int,
    ownerName: String,
    groupName: String,
    perms: HFsPermission,
    symlinkTarget: String,
    overwrite: Boolean,
    defaultBlockSize: Long,
    override val index: Long,
    override val time: Time
  ) extends HDFSEvent(source, index, time) {

  override def getEventType: EventType = EventType.CREATE

  override def toINotifyEvent: CreateEvent = {
    new CreateEvent.Builder()
      .iNodeType(iNodeType)
      .path(path)
      .ctime(ctime)
      .replication(replication)
      .ownerName(ownerName)
      .groupName(groupName)
      .perms(perms.toFsPermission)
      .symlinkTarget(symlinkTarget)
      .overwrite(overwrite)
      .defaultBlockSize(defaultBlockSize)
      .build()
  }

}

/**
 * Sent when there is an update to directory or file (none of the metadata
 * tracked here applies to symlinks) that is not associated with another
 * inotify event. The tracked metadata includes atime/mtime, replication,
 * owner/group, permissions, ACLs, and XAttributes. Fields not relevant to the
 * metadataType of the MetadataUpdateEvent will be null or will have their default
 * values.
 */
case class HDFSMetadataUpdateEvent(
    source: EventSource,
    path: String,
    metadataType: MetadataType,
    mtime: Long,
    atime: Long,
    replication: Int,
    ownerName: String,
    groupName: String,
    perms: HFsPermission,
    acls: List[HAclEntry],
    xAttrs: List[HXAttr],
    xAttrsRemoved: Boolean,
    override val index: Long,
    override val time: Time
  ) extends HDFSEvent(source, index, time) {

  override def getEventType: EventType = EventType.METADATA

  override def toINotifyEvent: MetadataUpdateEvent = {
    new MetadataUpdateEvent.Builder()
      .path(path)
      .metadataType(metadataType)
      .mtime(mtime)
      .atime(atime)
      .replication(replication)
      .ownerName(ownerName)
      .groupName(groupName)
      .perms(perms.toFsPermission)
      .acls(JavaConversions.seqAsJavaList(acls.map(_.toAclEntry)))
      .xAttrs(JavaConversions.seqAsJavaList(xAttrs.map(_.toXAttr)))
      .xAttrsRemoved(xAttrsRemoved)
      .build()
  }

}

/** Sent when a file, directory, or symlink is renamed. */
case class HDFSRenameEvent(
    source: EventSource,
    srcPath: String,
    dstPath: String,
    timestamp: Long,
    override val index: Long,
    override val time: Time
  ) extends HDFSEvent(source, index, time) {

  override def getEventType: EventType = EventType.RENAME

  override def toINotifyEvent: RenameEvent = {
    new RenameEvent.Builder()
      .srcPath(srcPath)
      .dstPath(dstPath)
      .timestamp(timestamp)
      .build()
  }

}

/** Sent when an existing file is opened for append. */
case class HDFSAppendEvent(
    source: EventSource,
    path: String,
    newBlock: Boolean,
    override val index: Long,
    override val time: Time
  ) extends HDFSEvent(source, index, time) {

  override def getEventType: EventType = EventType.APPEND

  override def toINotifyEvent: AppendEvent = {
    new AppendEvent.Builder()
      .path(path)
      .newBlock(newBlock)
      .build()
  }

}

/** Sent when a file, directory, or symlink is deleted. */
case class HDFSUnlinkEvent(
    source: EventSource,
    path: String,
    timestamp: Long,
    override val index: Long,
    override val time: Time
  ) extends HDFSEvent(source, index, time) {

  override def getEventType: EventType = EventType.UNLINK

  override def toINotifyEvent: UnlinkEvent = {
    new UnlinkEvent.Builder()
      .path(path)
      .timestamp(timestamp)
      .build()
  }

}

class HDFSEventSource(
    @transient ssc: StreamingContext,
    hdfsURI: String,
    path: String,
    name: String
  ) extends EventSource(ssc, name) {

  private var stopped = false
  private val indexCounter = new AtomicLong(0L)
  private var lastReadTxid = 0L

  @transient private var inotifyEventStream: DFSInotifyEventInputStream = null
  @transient private var pathPattern: GlobPattern = null

  private def init(): Unit = {
    try {
      val hdfsAdmin = new HdfsAdmin(URI.create(hdfsURI), ssc.sparkContext.hadoopConfiguration)
      inotifyEventStream = hdfsAdmin.getInotifyEventStream(lastReadTxid)
      pathPattern = new GlobPattern(path)
    } catch {
      case e: Exception =>
        throw new SparkException(s"Could not initialize ${this.toString}", e)
    }
  }

  override def start(): Unit = synchronized {
    init()
    stopped = false
    new Thread(this.toString) {
      setDaemon(true)
      override def run() { loop() }
    }.start()
    logDebug(s"${this.toString} started")
  }

  private def paths(iNotifyEvent: INotifyEvent): Seq[String] = iNotifyEvent match {
    case e: CloseEvent => Seq(e.getPath)
    case e: CreateEvent => Seq(e.getPath)
    case e: MetadataUpdateEvent => Seq(e.getPath)
    case e: RenameEvent => Seq(e.getSrcPath, e.getDstPath)
    case e: AppendEvent => Seq(e.getPath)
    case e: UnlinkEvent => Seq(e.getPath)
  }

  private def loop(): Unit = {
    val clock = context.clock
    while (!stopped) {
      Try(inotifyEventStream.take()) match {
        case Success(eventBatch) =>
          lastReadTxid = eventBatch.getTxid
          val events = eventBatch.getEvents
            .filter(paths(_).exists(pathPattern.matches))
            .map(HDFSEvent(this, _, indexCounter.getAndIncrement(), Time(clock.getTimeMillis())))
          events.foreach(post)
          logDebug(s"${this.toString} emitted ${events.length} event(s)")

        case Failure(e) =>
          logWarning(s"${this.toString} encountered exception while retrieving" +
            s" the next batch of events in the stream", e)
      }
    }
    stop()
  }

  override def restart() = start()

  override def stop(): Unit = synchronized {
    if (!stopped) {
      stopped = true
      logDebug(s"${this.toString} stopped")
    }
  }

  override def toProduct: Product = (getClass.getSimpleName, name, hdfsURI, path)

  override def toDetailedString: String = {
    s"${getClass.getSimpleName}($name,$hdfsURI,$path)"
  }

}

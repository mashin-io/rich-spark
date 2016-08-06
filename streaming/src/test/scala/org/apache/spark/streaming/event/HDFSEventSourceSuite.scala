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

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.hdfs.inotify.Event.EventType
import org.scalatest.concurrent.Eventually._

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, StreamingContext, TestSuiteBase}
import org.apache.spark.util.Utils

class HDFSEventSourceSuite extends TestSuiteBase {

  var dfs: MiniDFSCluster = null
  var dfsBaseDir: File = null
  var fs: FileSystem = null

  def setupContext(appName: String, batchDuration: Duration): StreamingContext = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(appName)
    new StreamingContext(conf, batchDuration)
  }

  def startDFS(): MiniDFSCluster = {
    dfsBaseDir = Utils.createTempDir()

    val conf = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dfsBaseDir.getAbsolutePath)

    val dfsClusterBuilder = new MiniDFSCluster.Builder(conf)
      .nameNodePort(8020).numDataNodes(4)

    dfs = dfsClusterBuilder.build()
    fs = dfs.getFileSystem

    dfs
  }

  def stopDFS(): Unit = {
    if (dfs != null) {
      dfs.shutdown()
    }
    if (dfsBaseDir != null) {
      Utils.deleteRecursively(dfsBaseDir)
    }
  }

  override def beforeFunction() {
    super.beforeFunction()
    startDFS()
  }

  override def afterFunction() {
    super.afterFunction()
    stopDFS()
  }

  test("hdfs event source") {
    withStreamingContext(setupContext("hdfs event source", Duration(100))) { ssc =>

      val rootDir = "/test/"
      val sleepDuration = 500
      val filesCount = 5

      val hdfsWatcher = ssc.hdfsWatcher(dfs.getURI.toString, rootDir + "*", "hdfsWatcher")
        .filter { case e: HDFSEvent => e.getEventType == EventType.CLOSE }

      val counter = new AtomicInteger(0)

      ssc.textFileStream(dfs.getURI.toString + rootDir)
        .foreachRDD { (rdd: RDD[String], event: Event) =>
          event match {
            case FilteredEvent(_, e: HDFSCloseEvent) =>
              assert(rdd.count === 1)
              val fileContent = rdd.first
              assert(e.path.equals(fileContent), s"'${e.path}' != '$fileContent'")
              counter.getAndIncrement()
            case _ =>
              fail(s"Did not expect event $event")
          }
          ()
        }
        .bind(hdfsWatcher)

      ssc.start()

      (1 to filesCount).foreach { i =>
        Thread.sleep(sleepDuration)
        val filePath = rootDir + i
        val dos = fs.create(new Path(filePath))
        dos.writeBytes(filePath)
        Thread.sleep(sleepDuration)
        dos.close()
      }

      eventually(eventuallyTimeout) {
        assert(counter.get === filesCount)
      }
    }
  }

}

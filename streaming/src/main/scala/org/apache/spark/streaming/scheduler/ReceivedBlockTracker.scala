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

import java.nio.ByteBuffer

import scala.collection.mutable
import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.streaming.Time
import org.apache.spark.streaming.util.{WriteAheadLog, WriteAheadLogUtils}
import org.apache.spark.util.{Clock, Utils}
import org.apache.spark.{Logging, SparkConf}

/** Trait representing any event in the ReceivedBlockTracker that updates its state. */
private[streaming] sealed trait ReceivedBlockTrackerLogEvent

private[streaming] case class BlockAdditionEvent(receivedBlockInfo: ReceivedBlockInfo)
  extends ReceivedBlockTrackerLogEvent
private[streaming] case class BatchAllocationEvent(time: Time, allocatedBlocks: AllocatedBlocks)
  extends ReceivedBlockTrackerLogEvent
private[streaming] case class BatchCleanupEvent(times: Seq[Time])
  extends ReceivedBlockTrackerLogEvent


/** Class representing the blocks of all the streams allocated to a batch */
private[streaming]
case class AllocatedBlocks(streamIdToAllocatedBlocks: Map[Int, Seq[ReceivedBlockInfo]]) {
  def getBlocksOfStream(streamId: Int): Seq[ReceivedBlockInfo] = {
    streamIdToAllocatedBlocks.getOrElse(streamId, Seq.empty)
  }
}

/**
 * Class that keep track of all the received blocks, and allocate them to batches
 * when required. All actions taken by this class can be saved to a write ahead log
 * (if a checkpoint directory has been provided), so that the state of the tracker
 * (received blocks and block-to-batch allocations) can be recovered after driver failure.
 *
 * Note that when any instance of this class is created with a checkpoint directory,
 * it will try reading events from logs in the directory.
 */
private[streaming] class ReceivedBlockTracker(
    conf: SparkConf,
    hadoopConf: Configuration,
    streamIds: Seq[Int],
    clock: Clock,
    recoverFromWriteAheadLog: Boolean,
    checkpointDirOption: Option[String])
  extends Logging {

  private type ReceivedBlockQueue = mutable.Queue[ReceivedBlockInfo]

  private val streamIdToUnallocatedBlockQueues = new mutable.HashMap[Int, ReceivedBlockQueue]
  private val timeToAllocatedBlocks = new mutable.HashMap[Time, AllocatedBlocks]
  private val writeAheadLogOption = createWriteAheadLog()
  private var lastAllocatedBatchTime: Time = null

  //xin the number of records per second
  val emptyJobTimestamp = new mutable.ArrayBuffer[Long]()
  val streamIdToRates = new mutable.HashMap[Int, Long]
  private case class BatchNum(jobTime: Long, numRecords: Long)
  private type TimeBlocksHashMap = mutable.HashMap[Long, ReceivedBlockQueue]
  private type RateQueue = mutable.Queue[BatchNum]
  private val streamIdTimeBlockQueues = new mutable.HashMap[Int, TimeBlocksHashMap]
  // memorize the rate for each job, just to check
  private val streamIdRateQueues = new mutable.HashMap[Int, RateQueue]

  // Recover block information from write ahead logs
  if (recoverFromWriteAheadLog) {
    recoverPastEvents()
  }

  /** Add received block. This event will get written to the write ahead log (if enabled). */
  def addBlock(receivedBlockInfo: ReceivedBlockInfo): Boolean = synchronized {
    try {
      writeToLog(BlockAdditionEvent(receivedBlockInfo))
      //xin
      val batchTime = receivedBlockInfo.getBatchTime
      if ( batchTime != -1 ){
        //streamIdTimeBlockQueues(receivedBlockInfo.streamId)(batchTime) += receivedBlockInfo
        getIdTimeQueue(receivedBlockInfo.streamId, batchTime) += receivedBlockInfo 
      } else {
        getReceivedBlockQueue(receivedBlockInfo.streamId) += receivedBlockInfo
      }
      
      logDebug(s"Stream ${receivedBlockInfo.streamId} received " +
        s"block ${receivedBlockInfo.blockStoreResult.blockId}")
      true
    } catch {
      case e: Exception =>
        logError(s"Error adding block $receivedBlockInfo", e)
        false
    }
  }

  //xin
  private def getRateQueue(streamId: Int): RateQueue = {
    streamIdRateQueues.getOrElseUpdate(streamId, new RateQueue)
  }
  private def getIdTimeHashMap(streamId: Int): TimeBlocksHashMap = {
    streamIdTimeBlockQueues.getOrElseUpdate(streamId, new TimeBlocksHashMap)
  } 
  private def getIdTimeQueue(streamId: Int, time: Long): ReceivedBlockQueue = {
    getIdTimeHashMap(streamId).getOrElseUpdate(time, new ReceivedBlockQueue) 
  } 
  def updateJobRate(streamId: Int, jobTime: Long, num: Long): Unit = {
    // assuming the jobtime comes in order
    val batchNum = new BatchNum( jobTime, num )
    //getRateQueue(streamId).enqueue( batchNum )
    getRateQueue(streamId) += batchNum 
  }
  def getBatchTimeBlocks(streamId: Int, batchTime: Long): Seq[ReceivedBlockInfo] = {
    if (emptyJobTimestamp.contains(batchTime)){
      emptyJobTimestamp.remove(emptyJobTimestamp.indexOf(batchTime))
      val buffer = mutable.ArrayBuffer[ReceivedBlockInfo]()
      val mylen = emptyJobTimestamp.length
      xinLogInfo(s"xin ReceivedBlockTracker zero element length: $mylen for time $batchTime")
      return buffer.toSeq 
    }
    val timeQueue = getIdTimeHashMap(streamId)
    if ( timeQueue.keySet.contains(batchTime ) ){
      //xin
      val qlen = timeQueue(batchTime).length
      val queueNum: Long = timeQueue(batchTime).map(_.numRecords.get).sum
      xinLogInfo(s"xin ReceivedBlockTracker myTimeQueue, Time: $batchTime NumBlocks: $qlen totalSize $queueNum")
      timeQueue(batchTime).dequeueAll(x => true)
    } else {
      val blockStr = getReceivedBlockQueue(streamId).map(_.numRecords.get).mkString(" ")
      val current = System.currentTimeMillis() 
      xinLogInfo(s"xin ReceivedBlockTracker allocate for batchTime $batchTime currentTime $current BlockLen $blockStr")
      getReceivedBlockQueue(streamId).dequeueAll(x => true)
      //takeFirstN(getReceivedBlockQueue(streamId), 5)
    }
  }
  def takeFirstN(myqueue: mutable.Queue[ReceivedBlockInfo], n: Int): Seq[ReceivedBlockInfo] ={
    if (n < 0 || myqueue.isEmpty)
      return myqueue.dequeueAll(x => true)
    var buffer = mutable.ArrayBuffer[ReceivedBlockInfo]()
    var myCount = 0
    while (!myqueue.isEmpty && myCount < n){
      val blockInfo = myqueue.dequeue()
      buffer += blockInfo 
      myCount += 1 
    }
    //xinLogInfo(s"xin ReceivedBlockTracker queue size: $queueNum, myCount $myCount specifiedRate $n qlength $qlen")
    return buffer.toSeq
  }
  def getElements(myqueue: mutable.Queue[ReceivedBlockInfo], n: Long): Seq[ReceivedBlockInfo] ={
    val qlen = myqueue.length
    val queueNum: Long = myqueue.map(_.numRecords.get).sum
    xinLogInfo(s"xin ReceivedBlockTracker queue size: $queueNum, qlength $qlen, specifiedRate $n")
    return myqueue.dequeueAll(x => true)

    if (n < 0 || myqueue.isEmpty)
      return myqueue.dequeueAll(x => true)
    var buffer = mutable.ArrayBuffer[ReceivedBlockInfo]()
    //add the first block, this job needs to have data 
    var myCount: Long = myqueue.front.numRecords.get 
    buffer += myqueue.dequeue()

    if (!myqueue.isEmpty)
      myCount += myqueue.front.numRecords.get
    while (!myqueue.isEmpty && myCount < n){
      val blockInfo = myqueue.dequeue()
      buffer += blockInfo 
      if (!myqueue.isEmpty) 
        myCount += myqueue.front.numRecords.get
      //myCount += blockInfo.numRecords.get
    }
    xinLogInfo(s"xin ReceivedBlockTracker queue size: $queueNum, myCount $myCount specifiedRate $n qlength $qlen")
    return buffer.toSeq
  }
    
  /**
   * Allocate all unallocated blocks to the given batch.
   * This event will get written to the write ahead log (if enabled).
   */
  def allocateBlocksToBatch(batchTime: Time): Unit = synchronized {
    if (lastAllocatedBatchTime == null || batchTime > lastAllocatedBatchTime) {
      val streamIdToBlocks = streamIds.map { streamId =>
          //(streamId, getReceivedBlockQueue(streamId).dequeueAll(x => true))
          //(streamId, getElements(getReceivedBlockQueue(streamId), streamIdToRates.getOrElseUpdate(streamId, -1)))
          (streamId, getBatchTimeBlocks(streamId, batchTime.milliseconds))
      }.toMap
      val allocatedBlocks = AllocatedBlocks(streamIdToBlocks)
      writeToLog(BatchAllocationEvent(batchTime, allocatedBlocks))
      timeToAllocatedBlocks(batchTime) = allocatedBlocks
      lastAllocatedBatchTime = batchTime
      allocatedBlocks
    } else {
      // This situation occurs when:
      // 1. WAL is ended with BatchAllocationEvent, but without BatchCleanupEvent,
      // possibly processed batch job or half-processed batch job need to be processed again,
      // so the batchTime will be equal to lastAllocatedBatchTime.
      // 2. Slow checkpointing makes recovered batch time older than WAL recovered
      // lastAllocatedBatchTime.
      // This situation will only occurs in recovery time.
      logInfo(s"Possibly processed batch $batchTime need to be processed again in WAL recovery")
    }
  }

  /** Get the blocks allocated to the given batch. */
  def getBlocksOfBatch(batchTime: Time): Map[Int, Seq[ReceivedBlockInfo]] = synchronized {
    timeToAllocatedBlocks.get(batchTime).map { _.streamIdToAllocatedBlocks }.getOrElse(Map.empty)
  }

  /** Get the blocks allocated to the given batch and stream. */
  def getBlocksOfBatchAndStream(batchTime: Time, streamId: Int): Seq[ReceivedBlockInfo] = {
    synchronized {
      timeToAllocatedBlocks.get(batchTime).map {
        _.getBlocksOfStream(streamId)
      }.getOrElse(Seq.empty)
    }
  }

  /** Check if any blocks are left to be allocated to batches. */
  def hasUnallocatedReceivedBlocks: Boolean = synchronized {
    !streamIdToUnallocatedBlockQueues.values.forall(_.isEmpty)
  }

  /**
   * Get blocks that have been added but not yet allocated to any batch. This method
   * is primarily used for testing.
   */
  def getUnallocatedBlocks(streamId: Int): Seq[ReceivedBlockInfo] = synchronized {
    getReceivedBlockQueue(streamId).toSeq
  }

  /**
   * Clean up block information of old batches. If waitForCompletion is true, this method
   * returns only after the files are cleaned up.
   */
  def cleanupOldBatches(cleanupThreshTime: Time, waitForCompletion: Boolean): Unit = synchronized {
    require(cleanupThreshTime.milliseconds < clock.getTimeMillis())
    //xin
    streamIdTimeBlockQueues.foreach{ case (id, timeHashMap) => timeHashMap.keys.filter{_ < cleanupThreshTime.milliseconds} }
    val timesToCleanup = timeToAllocatedBlocks.keys.filter { _ < cleanupThreshTime }.toSeq
    logInfo("Deleting batches " + timesToCleanup)
    writeToLog(BatchCleanupEvent(timesToCleanup))
    timeToAllocatedBlocks --= timesToCleanup
    writeAheadLogOption.foreach(_.clean(cleanupThreshTime.milliseconds, waitForCompletion))
  }

  /** Stop the block tracker. */
  def stop() {
    writeAheadLogOption.foreach { _.close() }
  }

  /**
   * Recover all the tracker actions from the write ahead logs to recover the state (unallocated
   * and allocated block info) prior to failure.
   */
  private def recoverPastEvents(): Unit = synchronized {
    // Insert the recovered block information
    def insertAddedBlock(receivedBlockInfo: ReceivedBlockInfo) {
      logTrace(s"Recovery: Inserting added block $receivedBlockInfo")
      receivedBlockInfo.setBlockIdInvalid()
      getReceivedBlockQueue(receivedBlockInfo.streamId) += receivedBlockInfo
    }

    // Insert the recovered block-to-batch allocations and clear the queue of received blocks
    // (when the blocks were originally allocated to the batch, the queue must have been cleared).
    def insertAllocatedBatch(batchTime: Time, allocatedBlocks: AllocatedBlocks) {
      logTrace(s"Recovery: Inserting allocated batch for time $batchTime to " +
        s"${allocatedBlocks.streamIdToAllocatedBlocks}")
      streamIdToUnallocatedBlockQueues.values.foreach { _.clear() }
      lastAllocatedBatchTime = batchTime
      timeToAllocatedBlocks.put(batchTime, allocatedBlocks)
    }

    // Cleanup the batch allocations
    def cleanupBatches(batchTimes: Seq[Time]) {
      logTrace(s"Recovery: Cleaning up batches $batchTimes")
      timeToAllocatedBlocks --= batchTimes
    }

    writeAheadLogOption.foreach { writeAheadLog =>
      logInfo(s"Recovering from write ahead logs in ${checkpointDirOption.get}")
      import scala.collection.JavaConversions._
      writeAheadLog.readAll().foreach { byteBuffer =>
        logTrace("Recovering record " + byteBuffer)
        Utils.deserialize[ReceivedBlockTrackerLogEvent](
          byteBuffer.array, Thread.currentThread().getContextClassLoader) match {
          case BlockAdditionEvent(receivedBlockInfo) =>
            insertAddedBlock(receivedBlockInfo)
          case BatchAllocationEvent(time, allocatedBlocks) =>
            insertAllocatedBatch(time, allocatedBlocks)
          case BatchCleanupEvent(batchTimes) =>
            cleanupBatches(batchTimes)
        }
      }
    }
  }

  /** Write an update to the tracker to the write ahead log */
  private def writeToLog(record: ReceivedBlockTrackerLogEvent) {
    if (isWriteAheadLogEnabled) {
      logDebug(s"Writing to log $record")
      writeAheadLogOption.foreach { logManager =>
        logManager.write(ByteBuffer.wrap(Utils.serialize(record)), clock.getTimeMillis())
      }
    }
  }

  /** Get the queue of received blocks belonging to a particular stream */
  private def getReceivedBlockQueue(streamId: Int): ReceivedBlockQueue = {
    streamIdToUnallocatedBlockQueues.getOrElseUpdate(streamId, new ReceivedBlockQueue)
  }

  /** Optionally create the write ahead log manager only if the feature is enabled */
  private def createWriteAheadLog(): Option[WriteAheadLog] = {
    checkpointDirOption.map { checkpointDir =>
      val logDir = ReceivedBlockTracker.checkpointDirToLogDir(checkpointDirOption.get)
      WriteAheadLogUtils.createLogForDriver(conf, logDir, hadoopConf)
    }
  }

  /** Check if the write ahead log is enabled. This is only used for testing purposes. */
  private[streaming] def isWriteAheadLogEnabled: Boolean = writeAheadLogOption.nonEmpty
}

private[streaming] object ReceivedBlockTracker {
  def checkpointDirToLogDir(checkpointDir: String): String = {
    new Path(checkpointDir, "receivedBlockMetadata").toString
  }
}

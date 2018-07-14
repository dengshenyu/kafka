/**
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
package kafka.log

import java.io.{File, IOException}
import java.nio.file.{Files, NoSuchFileException}
import java.nio.file.attribute.FileTime
import java.util.concurrent.TimeUnit

import kafka.common.LogSegmentOffsetOverflowException
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.server.epoch.LeaderEpochCache
import kafka.server.{FetchDataInfo, LogOffsetMetadata}
import kafka.utils._
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.record.FileRecords.LogOffsetPosition
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.math._

/**
 * A segment of the log. Each segment has two components: a log and an index. The log is a FileRecords containing
 * the actual messages. The index is an OffsetIndex that maps from logical offsets to physical file positions. Each
 * segment has a base offset which is an offset <= the least offset of any message in this segment and > any offset in
 * any previous segment.
 *
 * A segment with a base offset of [base_offset] would be stored in two files, a [base_offset].index and a [base_offset].log file.
 *
 * @param log The file records containing log entries
 * @param offsetIndex The offset index
 * @param timeIndex The timestamp index
 * @param baseOffset A lower bound on the offsets in this segment
 * @param indexIntervalBytes The approximate number of bytes between entries in the index
 * @param time The time instance
 *
 * 一个日志段. 每个日志段有两部分: 日志本身和索引. 日志由包含真正消息的FileRecords组成, 索引则负责将消息位移转换成文件内的物理偏移.
 * 每个日志段都有一个基准位移, 该基准位移小于等于此日志段中任何消息位移, 且大于之前日志段的任何消息位移.
 *
 * 假设日志段的基准位移为base_offset, 日志段会存储成两个文件, base_offset.index和base_offset.log文件.
 * 参数 log: 日志记录
 * 参数 offsetIndex: 位移索引
 * 参数 timeIndex: 时间戳索引
 * 参数 txnIndex: 事务索引
 * 参数 baseOffset: 此日志段的基准位移
 * 参数 indexIntervalBytes: 位移索引两个条目映射到日志段中相距的字节数
 * 参数 rollJitterMs:
 * 参数 maxSegmentMs: 日志段中消息的最大时间戳
 * 参数 maxSegmentBytes: 日志段的最大字节数
 * 参数 time: 用来获取时间的类
 */
@nonthreadsafe
class LogSegment private[log] (val log: FileRecords,
                               val offsetIndex: OffsetIndex,
                               val timeIndex: TimeIndex,
                               val txnIndex: TransactionIndex,
                               val baseOffset: Long,
                               val indexIntervalBytes: Int,
                               val rollJitterMs: Long,
                               val maxSegmentMs: Long,
                               val maxSegmentBytes: Int,
                               val time: Time) extends Logging {

  /**
   * 判断是否需要滚动日志段
   * @param messagesSize
   * @param maxTimestampInMessages
   * @param maxOffsetInMessages
   * @param now
   * @return
   */
  def shouldRoll(messagesSize: Int, maxTimestampInMessages: Long, maxOffsetInMessages: Long, now: Long): Boolean = {
    //根据时间跨度判断是否需要滚动日志
    val reachedRollMs = timeWaitedForRoll(now, maxTimestampInMessages) > maxSegmentMs - rollJitterMs
    //根据日志段大小, 日志段时间跨度, 索引大小以及位移是否溢出等情况判断是否需要滚动日志
    size > maxSegmentBytes - messagesSize ||
      (size > 0 && reachedRollMs) ||
      offsetIndex.isFull || timeIndex.isFull || !canConvertToRelativeOffset(maxOffsetInMessages)
  }

  def resizeIndexes(size: Int): Unit = {
    offsetIndex.resize(size)
    timeIndex.resize(size)
  }

  def sanityCheck(timeIndexFileNewlyCreated: Boolean): Unit = {
    if (offsetIndex.file.exists) {
      offsetIndex.sanityCheck()
      // Resize the time index file to 0 if it is newly created.
      if (timeIndexFileNewlyCreated)
        timeIndex.resize(0)
      timeIndex.sanityCheck()
      txnIndex.sanityCheck()
    }
    else throw new NoSuchFileException(s"Offset index file ${offsetIndex.file.getAbsolutePath} does not exist")
  }

  private var created = time.milliseconds

  /* the number of bytes since we last added an entry in the offset index */
  /* 自从上一次增加位移索引后, 日志段增加的消息字节数 */
  private var bytesSinceLastIndexEntry = 0

  /* The timestamp we used for time based log rolling */
  /* 日志段的起始时间戳 */
  private var rollingBasedTimestamp: Option[Long] = None

  /* The maximum timestamp we see so far */
  @volatile private var maxTimestampSoFar: Long = timeIndex.lastEntry.timestamp
  @volatile private var offsetOfMaxTimestamp: Long = timeIndex.lastEntry.offset

  /* Return the size in bytes of this log segment */
  /* 发怒会此日志段的字节数 */
  def size: Int = log.sizeInBytes()

  /**
   * checks that the argument offset can be represented as an integer offset relative to the baseOffset.
   */
  def canConvertToRelativeOffset(offset: Long): Boolean = {
    offsetIndex.canAppendOffset(offset)
  }

  /**
   * Append the given messages starting with the given offset. Add
   * an entry to the index if needed.
   *
   * It is assumed this method is being called from within a lock.
   *
   * @param largestOffset The last offset in the message set
   * @param largestTimestamp The largest timestamp in the message set.
   * @param shallowOffsetOfMaxTimestamp The offset of the message that has the largest timestamp in the messages to append.
   * @param records The log entries to append.
   * @return the physical position in the file of the appended records
   * @throws LogSegmentOffsetOverflowException if the largest offset causes index offset overflow
   *
   * 追加消息集, 消息集以参数中指定的位移开始. 索引(可能)增加条目.
   *
   * 在调用此方法前需要加锁
   *
   * 参数 largestOffset: 消息集的最大位移
   * 参数 largestTimestamp: 消息集的最大时间戳
   * 参数 shallowOffsetOfMaxTimestamp: 消息中最大时间戳的位移
   * 参数 records: 需要增加的日志条目
   * 返回 追加的记录在文件内的物理偏移
   * 抛出LogSegmentOffsetOverflowException异常: 如果消息集中最大的位移导致日志段位移溢出
   */
  @nonthreadsafe
  def append(largestOffset: Long,
             largestTimestamp: Long,
             shallowOffsetOfMaxTimestamp: Long,
             records: MemoryRecords): Unit = {
    if (records.sizeInBytes > 0) {
      trace(s"Inserting ${records.sizeInBytes} bytes at end offset $largestOffset at position ${log.sizeInBytes} " +
            s"with largest timestamp $largestTimestamp at shallow offset $shallowOffsetOfMaxTimestamp")
      val physicalPosition = log.sizeInBytes()

      //记录日志段的起始时间戳
      if (physicalPosition == 0)
        rollingBasedTimestamp = Some(largestTimestamp)

      //检查位移是否溢出
      ensureOffsetInRange(largestOffset)

      // append the messages
      val appendedBytes = log.append(records)
      trace(s"Appended $appendedBytes to ${log.file} at end offset $largestOffset")
      // Update the in memory max timestamp and corresponding offset.
      // 更新最大的时间戳及其位移
      if (largestTimestamp > maxTimestampSoFar) {
        maxTimestampSoFar = largestTimestamp
        offsetOfMaxTimestamp = shallowOffsetOfMaxTimestamp
      }
      // append an entry to the index (if needed)
      // 如果日志段新增了一定数量的消息, 那么增加一个位移索引, 同时(可能)增加时间戳索引
      if (bytesSinceLastIndexEntry > indexIntervalBytes) {
        //增加位移索引
        offsetIndex.append(largestOffset, physicalPosition)
        //(可能)增加时间戳索引
        timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp)

        bytesSinceLastIndexEntry = 0
      }
      bytesSinceLastIndexEntry += records.sizeInBytes
    }
  }

  private def ensureOffsetInRange(offset: Long): Unit = {
    if (!canConvertToRelativeOffset(offset))
      throw new LogSegmentOffsetOverflowException(this, offset)
  }

  private def appendChunkFromFile(records: FileRecords, position: Int, bufferSupplier: BufferSupplier): Int = {
    var bytesToAppend = 0
    var maxTimestamp = Long.MinValue
    var offsetOfMaxTimestamp = Long.MinValue
    var maxOffset = Long.MinValue
    var readBuffer = bufferSupplier.get(1024 * 1024)

    def canAppend(batch: RecordBatch) =
      canConvertToRelativeOffset(batch.lastOffset) &&
        (bytesToAppend == 0 || bytesToAppend + batch.sizeInBytes < readBuffer.capacity)

    // find all batches that are valid to be appended to the current log segment and
    // determine the maximum offset and timestamp
    val nextBatches = records.batchesFrom(position).asScala.iterator
    for (batch <- nextBatches.takeWhile(canAppend)) {
      if (batch.maxTimestamp > maxTimestamp) {
        maxTimestamp = batch.maxTimestamp
        offsetOfMaxTimestamp = batch.lastOffset
      }
      maxOffset = batch.lastOffset
      bytesToAppend += batch.sizeInBytes
    }

    if (bytesToAppend > 0) {
      // Grow buffer if needed to ensure we copy at least one batch
      if (readBuffer.capacity < bytesToAppend)
        readBuffer = bufferSupplier.get(bytesToAppend)

      readBuffer.limit(bytesToAppend)
      records.readInto(readBuffer, position)

      append(maxOffset, maxTimestamp, offsetOfMaxTimestamp, MemoryRecords.readableRecords(readBuffer))
    }

    bufferSupplier.release(readBuffer)
    bytesToAppend
  }

  /**
   * Append records from a file beginning at the given position until either the end of the file
   * is reached or an offset is found which is too large to convert to a relative offset for the indexes.
   *
   * @return the number of bytes appended to the log (may be less than the size of the input if an
   *         offset is encountered which would overflow this segment)
   */
  def appendFromFile(records: FileRecords, start: Int): Int = {
    var position = start
    val bufferSupplier: BufferSupplier = new BufferSupplier.GrowableBufferSupplier
    while (position < start + records.sizeInBytes) {
      val bytesAppended = appendChunkFromFile(records, position, bufferSupplier)
      if (bytesAppended == 0)
        return position - start
      position += bytesAppended
    }
    position - start
  }

  @nonthreadsafe
  def updateTxnIndex(completedTxn: CompletedTxn, lastStableOffset: Long) {
    if (completedTxn.isAborted) {
      trace(s"Writing aborted transaction $completedTxn to transaction index, last stable offset is $lastStableOffset")
      txnIndex.append(new AbortedTxn(completedTxn, lastStableOffset))
    }
  }

  private def updateProducerState(producerStateManager: ProducerStateManager, batch: RecordBatch): Unit = {
    if (batch.hasProducerId) {
      val producerId = batch.producerId
      val appendInfo = producerStateManager.prepareUpdate(producerId, isFromClient = false)
      val maybeCompletedTxn = appendInfo.append(batch)
      producerStateManager.update(appendInfo)
      maybeCompletedTxn.foreach { completedTxn =>
        val lastStableOffset = producerStateManager.completeTxn(completedTxn)
        updateTxnIndex(completedTxn, lastStableOffset)
      }
    }
    producerStateManager.updateMapEndOffset(batch.lastOffset + 1)
  }

  /**
   * Find the physical file position for the first message with offset >= the requested offset.
   *
   * The startingFilePosition argument is an optimization that can be used if we already know a valid starting position
   * in the file higher than the greatest-lower-bound from the index.
   *
   * @param offset The offset we want to translate
   * @param startingFilePosition A lower bound on the file position from which to begin the search. This is purely an optimization and
   * when omitted, the search will begin at the position in the offset index.
   * @return The position in the log storing the message with the least offset >= the requested offset and the size of the
   *         message or null if no message meets this criteria.
   *
   * 查找第一条位移大于等于参数offset的消息, 返回它在日志文件中的物理偏移.
   * startingFilePosition是一个优化参数, 如果我们已经知道查找的物理起始位移时可以传入
   *
   * 参数 offset: 想要转换的消息位移
   * 参数 startingFilePosition: 查找文件时可以指定的开始位置. 这是一个加速查找的优化参数.
   * 返回 第一条位移大于等于参数offset的消息的物理偏移, 以及其消息大小
   */
  @threadsafe
  private[log] def translateOffset(offset: Long, startingFilePosition: Int = 0): LogOffsetPosition = {
    //查找不大于参数offset的最大位移
    val mapping = offsetIndex.lookup(offset)

    //查找第一条位移大于等于参数offset的消息, 返回它在日志文件中的物理偏移以及大小(包括日志存储额外开销)
    log.searchForOffsetWithSize(offset, max(mapping.position, startingFilePosition))
  }

  /**
   * Read a message set from this segment beginning with the first offset >= startOffset. The message set will include
   * no more than maxSize bytes and will end before maxOffset if a maxOffset is specified.
   *
   * @param startOffset A lower bound on the first offset to include in the message set we read
   * @param maxOffset An optional maximum offset for the message set we read
   * @param maxSize The maximum number of bytes to include in the message set we read
   * @param maxPosition The maximum position in the log segment that should be exposed for read
   * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxSize` (if one exists)
   *
   * @return The fetched data and the offset metadata of the first message whose offset is >= startOffset,
   *         or null if the startOffset is larger than the largest offset in this log
   *
   * 从参数startOffset开始读取消息, 读取的消息大小不超过参数maxSize, 而且位移不大于maxOffset
   * 参数 startOffset: 读取的起始消息位移
   * 参数 maxOffset: 可选参数, 读取的最大位移
   * 参数 maxSize: 可选参数, 读取的最大字节数
   * 参数 maxPosition: 日志段中能读取的最大物理偏移
   * 参数 minOneMessage: 如果为true, 那么至少返回一条消息(即使返回的消息超出`maxSize`)
   * 返回 拉取的数据, 以及第一条消息的位移元数据(如果起始位移大于此日志段最大位移则为null)
   */
  @threadsafe
  def read(startOffset: Long, maxOffset: Option[Long], maxSize: Int, maxPosition: Long = size,
           minOneMessage: Boolean = false): FetchDataInfo = {
    //校验maxSize
    if (maxSize < 0)
      throw new IllegalArgumentException(s"Invalid max size $maxSize for log read from segment $log")

    //日志大小可能由于写入而改变, 这里保存一个快照以保证一致性
    val logSize = log.sizeInBytes // this may change, need to save a consistent copy

    //查找第一条位移大于等于startOffset的消息, 获取它在日志文件中的物理偏移及大小.
    val startOffsetAndSize = translateOffset(startOffset)

    // if the start position is already off the end of the log, return null
    // 如果没找到这样的消息, 那么返回null
    if (startOffsetAndSize == null)
      return null

    //生成第一个消息的位移元数据, 包括(消息位移, 日志段基准位移, 消息在日志内的物理偏移)
    val startPosition = startOffsetAndSize.position
    val offsetMetadata = new LogOffsetMetadata(startOffset, this.baseOffset, startPosition)

    //如果设置了minOneMessage, 那么保证至少返回一条消息, 因此这里调整maxSize
    val adjustedMaxSize =
      if (minOneMessage) math.max(maxSize, startOffsetAndSize.size)
      else maxSize

    // return a log segment but with zero size in the case below
    // 如果adjustedMaxSize为0, 那么直接返回空数据
    if (adjustedMaxSize == 0)
      return FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY)

    // calculate the length of the message set to read based on whether or not they gave us a maxOffset
    // 根据maxOffset判断要返回的数据大小
    val fetchSize: Int = maxOffset match {
      //如果没有设置maxOffset, 那么一直读取直到maxPosition或者大小达到adjustedMaxSize
      case None =>
        min((maxPosition - startPosition).toInt, adjustedMaxSize)

      // there is a max offset, translate it to a file position and use that to calculate the max read size;
      // when the leader of a partition changes, it's possible for the new leader's high watermark to be less than the
      // true high watermark in the previous leader for a short window. In this window, if a consumer fetches on an
      // offset between new leader's high watermark and the log end offset, we want to return an empty response.
      // 如果设置了最大位移(maxOffset), 那么将它转换成文件内的物理偏移, 然后根据该物理偏移计算读取的最大大小;
      // 当分区的leader改变时, 有可能在一小段时间内新leader的高水位线小于老leader的高水位线, 如果一个消费者在
      // 这段时间内拉取超出新leader的高水位线的消息时, 这里返回空数据
      case Some(offset) =>
        if (offset < startOffset)
          return FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY, firstEntryIncomplete = false)
        val mapping = translateOffset(offset, startPosition)
        val endPosition =
          if (mapping == null)
            logSize // the max offset is off the end of the log, use the end of the file
          else
            mapping.position
        min(min(maxPosition, endPosition) - startPosition, adjustedMaxSize).toInt
    }

    //返回拉取的数据信息(第一条消息条目的位移元数据, 拉取的消息视图, 指定了maxSize时第一个消息条目是否完整)
    FetchDataInfo(offsetMetadata, log.slice(startPosition, fetchSize),
      firstEntryIncomplete = adjustedMaxSize < startOffsetAndSize.size)
  }

  /**
   * 获取一个上限的位移
   * @param startOffsetPosition
   * @param fetchSize
   * @return
   */
   def fetchUpperBoundOffset(startOffsetPosition: OffsetPosition, fetchSize: Int): Option[Long] =
     offsetIndex.fetchUpperBoundOffset(startOffsetPosition, fetchSize).map(_.offset)

  /**
   * Run recovery on the given segment. This will rebuild the index from the log file and lop off any invalid bytes
   * from the end of the log and index.
   *
   * @param producerStateManager Producer state corresponding to the segment's base offset. This is needed to recover
   *                             the transaction index.
   * @param leaderEpochCache Optionally a cache for updating the leader epoch during recovery.
   * @return The number of bytes truncated from the log
   * @throws LogSegmentOffsetOverflowException if the log segment contains an offset that causes the index offset to overflow
   */
  @nonthreadsafe
  def recover(producerStateManager: ProducerStateManager, leaderEpochCache: Option[LeaderEpochCache] = None): Int = {
    offsetIndex.reset()
    timeIndex.reset()
    txnIndex.reset()
    var validBytes = 0
    var lastIndexEntry = 0
    maxTimestampSoFar = RecordBatch.NO_TIMESTAMP
    try {
      for (batch <- log.batches.asScala) {
        batch.ensureValid()
        ensureOffsetInRange(batch.lastOffset)

        // The max timestamp is exposed at the batch level, so no need to iterate the records
        if (batch.maxTimestamp > maxTimestampSoFar) {
          maxTimestampSoFar = batch.maxTimestamp
          offsetOfMaxTimestamp = batch.lastOffset
        }

        // Build offset index
        if (validBytes - lastIndexEntry > indexIntervalBytes) {
          offsetIndex.append(batch.lastOffset, validBytes)
          timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp)
          lastIndexEntry = validBytes
        }
        validBytes += batch.sizeInBytes()

        if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) {
          leaderEpochCache.foreach { cache =>
            if (batch.partitionLeaderEpoch > cache.latestEpoch()) // this is to avoid unnecessary warning in cache.assign()
              cache.assign(batch.partitionLeaderEpoch, batch.baseOffset)
          }
          updateProducerState(producerStateManager, batch)
        }
      }
    } catch {
      case e: CorruptRecordException =>
        warn("Found invalid messages in log segment %s at byte offset %d: %s."
          .format(log.file.getAbsolutePath, validBytes, e.getMessage))
    }
    val truncated = log.sizeInBytes - validBytes
    if (truncated > 0)
      debug(s"Truncated $truncated invalid bytes at the end of segment ${log.file.getAbsoluteFile} during recovery")

    log.truncateTo(validBytes)
    offsetIndex.trimToValidSize()
    // A normally closed segment always appends the biggest timestamp ever seen into log segment, we do this as well.
    timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp, skipFullCheck = true)
    timeIndex.trimToValidSize()
    truncated
  }

  private def loadLargestTimestamp() {
    // Get the last time index entry. If the time index is empty, it will return (-1, baseOffset)
    val lastTimeIndexEntry = timeIndex.lastEntry
    maxTimestampSoFar = lastTimeIndexEntry.timestamp
    offsetOfMaxTimestamp = lastTimeIndexEntry.offset

    val offsetPosition = offsetIndex.lookup(lastTimeIndexEntry.offset)
    // Scan the rest of the messages to see if there is a larger timestamp after the last time index entry.
    val maxTimestampOffsetAfterLastEntry = log.largestTimestampAfter(offsetPosition.position)
    if (maxTimestampOffsetAfterLastEntry.timestamp > lastTimeIndexEntry.timestamp) {
      maxTimestampSoFar = maxTimestampOffsetAfterLastEntry.timestamp
      offsetOfMaxTimestamp = maxTimestampOffsetAfterLastEntry.offset
    }
  }

  /**
   * Check whether the last offset of the last batch in this segment overflows the indexes.
   */
  def hasOverflow: Boolean = {
    val nextOffset = readNextOffset
    nextOffset > baseOffset && !canConvertToRelativeOffset(nextOffset - 1)
  }

  /**
   * 获取拉取范围的已终止事务
   *
   * @param fetchOffset
   * @param upperBoundOffset
   * @return
   */
  def collectAbortedTxns(fetchOffset: Long, upperBoundOffset: Long): TxnIndexSearchResult =
    txnIndex.collectAbortedTxns(fetchOffset, upperBoundOffset)

  override def toString = "LogSegment(baseOffset=" + baseOffset + ", size=" + size + ")"

  /**
   * Truncate off all index and log entries with offsets >= the given offset.
   * If the given offset is larger than the largest message in this segment, do nothing.
   *
   * @param offset The offset to truncate to
   * @return The number of log bytes truncated
   */
  @nonthreadsafe
  def truncateTo(offset: Long): Int = {
    // Do offset translation before truncating the index to avoid needless scanning
    // in case we truncate the full index
    val mapping = translateOffset(offset)
    offsetIndex.truncateTo(offset)
    timeIndex.truncateTo(offset)
    txnIndex.truncateTo(offset)

    // After truncation, reset and allocate more space for the (new currently active) index
    offsetIndex.resize(offsetIndex.maxIndexSize)
    timeIndex.resize(timeIndex.maxIndexSize)

    val bytesTruncated = if (mapping == null) 0 else log.truncateTo(mapping.position)
    if (log.sizeInBytes == 0) {
      created = time.milliseconds
      rollingBasedTimestamp = None
    }

    bytesSinceLastIndexEntry = 0
    if (maxTimestampSoFar >= 0)
      loadLargestTimestamp()
    bytesTruncated
  }

  /**
   * Calculate the offset that would be used for the next message to be append to this segment.
   * Note that this is expensive.
   */
  @threadsafe
  def readNextOffset: Long = {
    val fetchData = read(offsetIndex.lastOffset, None, log.sizeInBytes)
    if (fetchData == null)
      baseOffset
    else
      fetchData.records.batches.asScala.lastOption
        .map(_.nextOffset)
        .getOrElse(baseOffset)
  }

  /**
   * Flush this log segment to disk
   */
  @threadsafe
  def flush() {
    LogFlushStats.logFlushTimer.time {
      log.flush()
      offsetIndex.flush()
      timeIndex.flush()
      txnIndex.flush()
    }
  }

  /**
   * Update the directory reference for the log and indices in this segment. This would typically be called after a
   * directory is renamed.
   */
  def updateDir(dir: File): Unit = {
    log.setFile(new File(dir, log.file.getName))
    offsetIndex.file = new File(dir, offsetIndex.file.getName)
    timeIndex.file = new File(dir, timeIndex.file.getName)
    txnIndex.file = new File(dir, txnIndex.file.getName)
  }

  /**
   * Change the suffix for the index and log file for this log segment
   * IOException from this method should be handled by the caller
   */
  def changeFileSuffixes(oldSuffix: String, newSuffix: String) {
    log.renameTo(new File(CoreUtils.replaceSuffix(log.file.getPath, oldSuffix, newSuffix)))
    offsetIndex.renameTo(new File(CoreUtils.replaceSuffix(offsetIndex.file.getPath, oldSuffix, newSuffix)))
    timeIndex.renameTo(new File(CoreUtils.replaceSuffix(timeIndex.file.getPath, oldSuffix, newSuffix)))
    txnIndex.renameTo(new File(CoreUtils.replaceSuffix(txnIndex.file.getPath, oldSuffix, newSuffix)))
  }

  /**
   * Append the largest time index entry to the time index and trim the log and indexes.
   *
   * The time index entry appended will be used to decide when to delete the segment.
   * 在关闭日志段前调用此方法, 此方法会在时间索引中增加一个最大的时间戳索引, 并trim日志文件, 位移索引文件和时间索引文件
   */
  def onBecomeInactiveSegment() {
    //在时间戳文件中新增最大时间戳的索引条目
    timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp, skipFullCheck = true)
    //trim索引文件
    offsetIndex.trimToValidSize()
    //trim时间索引文件
    timeIndex.trimToValidSize()
    //trim日志文件
    log.trim()
  }

  /**
   * The time this segment has waited to be rolled.
   * If the first message batch has a timestamp we use its timestamp to determine when to roll a segment. A segment
   * is rolled if the difference between the new batch's timestamp and the first batch's timestamp exceeds the
   * segment rolling time.
   * If the first batch does not have a timestamp, we use the wall clock time to determine when to roll a segment. A
   * segment is rolled if the difference between the current wall clock time and the segment create time exceeds the
   * segment rolling time.
   *
   * 日志段等待被滚动的时间.
   * 如果第一个消息batch有时间戳, 那么使用该时间戳来判断是否需要滚动. 如果新的batch时间戳与第一个batch的时间戳的差值超过阈值, 则需要滚动.
   * 如果第一个batch没有时间戳, 那么使用自然时间来判断是否需要滚动. 如果当前时间与日志段创建的时间差值超过阈值, 则需要滚动.
   */
  def timeWaitedForRoll(now: Long, messageTimestamp: Long) : Long = {
    // Load the timestamp of the first message into memory
    if (rollingBasedTimestamp.isEmpty) {
      val iter = log.batches.iterator()
      if (iter.hasNext)
        rollingBasedTimestamp = Some(iter.next().maxTimestamp)
    }
    rollingBasedTimestamp match {
      case Some(t) if t >= 0 => messageTimestamp - t
      case _ => now - created
    }
  }

  /**
   * Search the message offset based on timestamp and offset.
   *
   * This method returns an option of TimestampOffset. The returned value is determined using the following ordered list of rules:
   *
   * - If all the messages in the segment have smaller offsets, return None
   * - If all the messages in the segment have smaller timestamps, return None
   * - If all the messages in the segment have larger timestamps, or no message in the segment has a timestamp
   *   the returned the offset will be max(the base offset of the segment, startingOffset) and the timestamp will be Message.NoTimestamp.
   * - Otherwise, return an option of TimestampOffset. The offset is the offset of the first message whose timestamp
   *   is greater than or equals to the target timestamp and whose offset is greater than or equals to the startingOffset.
   *
   * This methods only returns None when 1) all messages' offset < startOffing or 2) the log is not empty but we did not
   * see any message when scanning the log from the indexed position. The latter could happen if the log is truncated
   * after we get the indexed position but before we scan the log from there. In this case we simply return None and the
   * caller will need to check on the truncated log and maybe retry or even do the search on another log segment.
   *
   * @param timestamp The timestamp to search for.
   * @param startingOffset The starting offset to search.
   * @return the timestamp and offset of the first message that meets the requirements. None will be returned if there is no such message.
   *
   * 基于时间戳和位移, 查询消息的位移.
   *
   * 此方法返回一个Option[TimestampOffset], 返回的值由如下步骤决定:
   *
   * 1) 如果此日志段所有消息的位移比参数startingOffset小, 那么返回None;
   * 2) 如果此日志段所有消息的时间戳比参数startingOffset小, 那么返回None;
   * 3) 如果此日志段所有消息的时间戳都比参数timestamp大, 或者此日志段的消息没有时间戳, 那么返回的位移为max(日志段基准位移, startingOffset),
   *    而且时间戳为Message.NoTimestamp.
   * 4) 否则返回Option[TimestampOffset], 其位移为第一条时间戳大于等于目标时间戳的消息位移, 而且该消息位移大于等于参数startingOffset.
   *
   * 另外, 如果日志不为空但从索引位置开始扫描不到任何消息时, 此方法也会返回None. 当我们获取到索引位置后但还没开始扫描时, 日志被截断了,
   * 就会出现这种情况. 对于这种情况, 我们只需要简单的返回None, 调用者需要检查日志阶段, 并根据情况决定是否重试或查询别的日志段.
   */
  def findOffsetByTimestamp(timestamp: Long, startingOffset: Long = baseOffset): Option[TimestampOffset] = {
    // Get the index entry with a timestamp less than or equal to the target timestamp
    // 获取时间戳小于等于目标时间戳的索引条目
    val timestampOffset = timeIndex.lookup(timestamp)

    //目标位移为max(参数startingOffset, 上面timestampOffset), 获取位移小于等于目标位移的最大文件内物理偏移
    val position = offsetIndex.lookup(math.max(timestampOffset.offset, startingOffset)).position

    // Search the timestamp
    // 从该物理偏移开始查询第一条满足如下条件的消息:
    // 1)消息时间戳需要大于等于目标时间戳大;
    // 2)位移大于等于指定起始位移
    Option(log.searchForTimestamp(timestamp, position, startingOffset)).map { timestampAndOffset =>
      TimestampOffset(timestampAndOffset.timestamp, timestampAndOffset.offset)
    }
  }

  /**
   * Close this log segment
   */
  def close() {
    CoreUtils.swallow(timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp, skipFullCheck = true), this)
    CoreUtils.swallow(offsetIndex.close(), this)
    CoreUtils.swallow(timeIndex.close(), this)
    CoreUtils.swallow(log.close(), this)
    CoreUtils.swallow(txnIndex.close(), this)
  }

  /**
    * Close file handlers used by the log segment but don't write to disk. This is used when the disk may have failed
    */
  def closeHandlers() {
    CoreUtils.swallow(offsetIndex.closeHandler(), this)
    CoreUtils.swallow(timeIndex.closeHandler(), this)
    CoreUtils.swallow(log.closeHandlers(), this)
    CoreUtils.swallow(txnIndex.close(), this)
  }

  /**
   * Delete this log segment from the filesystem.
   * 在文件系统中删除此日志段
   */
  def deleteIfExists() {
    def delete(delete: () => Boolean, fileType: String, file: File, logIfMissing: Boolean): Unit = {
      try {
        if (delete())
          info(s"Deleted $fileType ${file.getAbsolutePath}.")
        else if (logIfMissing)
          info(s"Failed to delete $fileType ${file.getAbsolutePath} because it does not exist.")
      }
      catch {
        case e: IOException => throw new IOException(s"Delete of $fileType ${file.getAbsolutePath} failed.", e)
      }
    }

    CoreUtils.tryAll(Seq(
      () => delete(log.deleteIfExists _, "log", log.file, logIfMissing = true),
      () => delete(offsetIndex.deleteIfExists _, "offset index", offsetIndex.file, logIfMissing = true),
      () => delete(timeIndex.deleteIfExists _, "time index", timeIndex.file, logIfMissing = true),
      () => delete(txnIndex.deleteIfExists _, "transaction index", txnIndex.file, logIfMissing = false)
    ))
  }

  /**
   * The last modified time of this log segment as a unix time stamp
   */
  def lastModified = log.file.lastModified

  /**
   * The largest timestamp this segment contains.
   */
  def largestTimestamp = if (maxTimestampSoFar >= 0) maxTimestampSoFar else lastModified

  /**
   * Change the last modified time for this log segment
   */
  def lastModified_=(ms: Long) = {
    val fileTime = FileTime.fromMillis(ms)
    Files.setLastModifiedTime(log.file.toPath, fileTime)
    Files.setLastModifiedTime(offsetIndex.file.toPath, fileTime)
    Files.setLastModifiedTime(timeIndex.file.toPath, fileTime)
  }

}

object LogSegment {

  /**
   * 打开(或创建)日志段
   * @param dir 日志段目录
   * @param baseOffset 日志段基准位移
   * @param config 日志段配置
   * @param time 用于获取时间的实例
   * @param fileAlreadyExists 文件是否存在
   * @param initFileSize 初始的文件大小
   * @param preallocate 是否预分配空间
   * @param fileSuffix 文件名后缀
   * @return 日志段实例
   */
  def open(dir: File, baseOffset: Long, config: LogConfig, time: Time, fileAlreadyExists: Boolean = false,
           initFileSize: Int = 0, preallocate: Boolean = false, fileSuffix: String = ""): LogSegment = {
    val maxIndexSize = config.maxIndexSize
    new LogSegment(
      //存储消息的类
      FileRecords.open(Log.logFile(dir, baseOffset, fileSuffix), fileAlreadyExists, initFileSize, preallocate),
      //存储位移索引的类
      new OffsetIndex(Log.offsetIndexFile(dir, baseOffset, fileSuffix), baseOffset = baseOffset, maxIndexSize = maxIndexSize),
      //存储时间戳索引的类
      new TimeIndex(Log.timeIndexFile(dir, baseOffset, fileSuffix), baseOffset = baseOffset, maxIndexSize = maxIndexSize),
      //存储(终止)事务索引的类
      new TransactionIndex(baseOffset, Log.transactionIndexFile(dir, baseOffset, fileSuffix)),
      baseOffset,
      indexIntervalBytes = config.indexInterval,
      rollJitterMs = config.randomSegmentJitter,
      maxSegmentMs = config.segmentMs,
      maxSegmentBytes = config.segmentSize,
      time)
  }

  def deleteIfExists(dir: File, baseOffset: Long, fileSuffix: String = ""): Unit = {
    Log.deleteFileIfExists(Log.offsetIndexFile(dir, baseOffset, fileSuffix))
    Log.deleteFileIfExists(Log.timeIndexFile(dir, baseOffset, fileSuffix))
    Log.deleteFileIfExists(Log.transactionIndexFile(dir, baseOffset, fileSuffix))
    Log.deleteFileIfExists(Log.logFile(dir, baseOffset, fileSuffix))
  }
}

object LogFlushStats extends KafkaMetricsGroup {
  val logFlushTimer = new KafkaTimer(newTimer("LogFlushRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
}

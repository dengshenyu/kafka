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
import java.lang.{Long => JLong}
import java.nio.file.{Files, NoSuchFileException}
import java.text.NumberFormat
import java.util.Map.{Entry => JEntry}
import java.util.concurrent.atomic._
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap, TimeUnit}
import java.util.regex.Pattern

import com.yammer.metrics.core.Gauge
import kafka.api.KAFKA_0_10_0_IV0
import kafka.common.{LogSegmentOffsetOverflowException, LongRef, OffsetsOutOfOrderException, UnexpectedAppendOffsetException}
import kafka.message.{BrokerCompressionCodec, CompressionCodec, NoCompressionCodec}
import kafka.metrics.KafkaMetricsGroup
import kafka.server.checkpoints.{LeaderEpochCheckpointFile, LeaderEpochFile}
import kafka.server.epoch.{LeaderEpochCache, LeaderEpochFileCache}
import kafka.server.{BrokerTopicStats, FetchDataInfo, LogDirFailureChannel, LogOffsetMetadata}
import kafka.utils._
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.errors.{CorruptRecordException, InvalidOffsetException, KafkaStorageException, OffsetOutOfRangeException, RecordBatchTooLargeException, RecordTooLargeException, UnsupportedForMessageFormatException}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction
import org.apache.kafka.common.requests.{IsolationLevel, ListOffsetRequest}
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.{Seq, Set, mutable}

object LogAppendInfo {
  val UnknownLogAppendInfo = LogAppendInfo(None, -1, RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP, -1L,
    RecordConversionStats.EMPTY, NoCompressionCodec, NoCompressionCodec, -1, -1, offsetsMonotonic = false, -1L)

  def unknownLogAppendInfoWithLogStartOffset(logStartOffset: Long): LogAppendInfo =
    LogAppendInfo(None, -1, RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP, logStartOffset,
      RecordConversionStats.EMPTY, NoCompressionCodec, NoCompressionCodec, -1, -1, offsetsMonotonic = false, -1L)
}

/**
 * Struct to hold various quantities we compute about each message set before appending to the log
 *
 * @param firstOffset The first offset in the message set unless the message format is less than V2 and we are appending
 *                    to the follower.
 * @param lastOffset The last offset in the message set
 * @param maxTimestamp The maximum timestamp of the message set.
 * @param offsetOfMaxTimestamp The offset of the message with the maximum timestamp.
 * @param logAppendTime The log append time (if used) of the message set, otherwise Message.NoTimestamp
 * @param logStartOffset The start offset of the log at the time of this append.
 * @param recordConversionStats Statistics collected during record processing, `null` if `assignOffsets` is `false`
 * @param sourceCodec The source codec used in the message set (send by the producer)
 * @param targetCodec The target codec of the message set(after applying the broker compression configuration if any)
 * @param shallowCount The number of shallow messages
 * @param validBytes The number of valid bytes
 * @param offsetsMonotonic Are the offsets in this message set monotonically increasing
 * @param lastOffsetOfFirstBatch The last offset of the first batch
 *
 * 在追加消息到日志前用来保存计算出来的各种信息的结构体
 * 参数 firstOffset: 消息集的第一个消息位移(小于V2版本且为追加到Follower中时除外)
 * 参数 lastOffset: 消息集的最后位移
 * 参数 maxTimestamp: 消息集的最大时间戳
 * 参数 offsetOfMaxTimestamp: 最大时间戳所对应的消息位移
 * 参数 logAppendTime: 消息集的日志追加时间, 或者为Message.NoTimestamp(如果此字段没有使用的话)
 * 参数 logStartOffset: 追加时此分区日志的最老位移
 * 参数 recordConversionStats: 消息记录处理的指标统计, 如果assignOffsets设置为false则此字段为null
 * 参数 sourceCodec: 消息集的原编码(由生产者设置)
 * 参数 targetCodec: 消息集的目标编码(如果broker设置了压缩的话)
 * 参数 shallowCount: 消息集中batch的个数
 * 参数 validBytes: 有效字节数
 * 参数 offsetsMonotonic: 消息集的位移是否为单调递增
 * 参数 lastOffsetOfFirstBatch: 第一个batch的最后位移
 */
case class LogAppendInfo(var firstOffset: Option[Long],
                         var lastOffset: Long,
                         var maxTimestamp: Long,
                         var offsetOfMaxTimestamp: Long,
                         var logAppendTime: Long,
                         var logStartOffset: Long,
                         var recordConversionStats: RecordConversionStats,
                         sourceCodec: CompressionCodec,
                         targetCodec: CompressionCodec,
                         shallowCount: Int,
                         validBytes: Int,
                         offsetsMonotonic: Boolean,
                         lastOffsetOfFirstBatch: Long) {
  /**
   * Get the first offset if it exists, else get the last offset of the first batch
   * For magic versions 2 and newer, this method will return first offset. For magic versions
   * older than 2, we use the last offset of the first batch as an approximation of the first
   * offset to avoid decompressing the data.
   *
   * 获取第一条消息的位移, 如果不存在则获取第一个batch的最后一条位移.
   * 对于magic版本大于等于2的, 此方法会返回第一条消息位移; 对于老的版本, 会使用第一个batch的最后位移以避免
   * 数据解压缩.
   */
  def firstOrLastOffsetOfFirstBatch: Long = firstOffset.getOrElse(lastOffsetOfFirstBatch)

  /**
   * Get the (maximum) number of messages described by LogAppendInfo
   * @return Maximum possible number of messages described by LogAppendInfo
   *
   * 获取LogAppendInfo中的消息最大数目
   */
  def numMessages: Long = {
    firstOffset match {
      case Some(firstOffsetVal) if (firstOffsetVal >= 0 && lastOffset >= 0) => (lastOffset - firstOffsetVal + 1)
      case _ => 0
    }
  }
}

/**
 * A class used to hold useful metadata about a completed transaction. This is used to build
 * the transaction index after appending to the log.
 *
 * @param producerId The ID of the producer
 * @param firstOffset The first offset (inclusive) of the transaction
 * @param lastOffset The last offset (inclusive) of the transaction. This is always the offset of the
 *                   COMMIT/ABORT control record which indicates the transaction's completion.
 * @param isAborted Whether or not the transaction was aborted
 *
 * 用来保存事务的元数据, 用来建立事务索引
 * 参数 producerId: 生产者ID
 * 参数 firstOffset: 事务的初始位移
 * 参数 lastOffset: 事务的最后位移, 通常为COMMIT/ABORT控制原语的位移
 * 参数 isAborted: 是否事务被回滚
 *
 */
case class CompletedTxn(producerId: Long, firstOffset: Long, lastOffset: Long, isAborted: Boolean) {
  override def toString: String = {
    "CompletedTxn(" +
      s"producerId=$producerId, " +
      s"firstOffset=$firstOffset, " +
      s"lastOffset=$lastOffset, " +
      s"isAborted=$isAborted)"
  }
}

/**
 * An append-only log for storing messages.
 *
 * The log is a sequence of LogSegments, each with a base offset denoting the first message in the segment.
 *
 * New log segments are created according to a configurable policy that controls the size in bytes or time interval
 * for a given segment.
 *
 * @param dir The directory in which log segments are created.
 * @param config The log configuration settings
 * @param logStartOffset The earliest offset allowed to be exposed to kafka client.
 *                       The logStartOffset can be updated by :
 *                       - user's DeleteRecordsRequest
 *                       - broker's log retention
 *                       - broker's log truncation
 *                       The logStartOffset is used to decide the following:
 *                       - Log deletion. LogSegment whose nextOffset <= log's logStartOffset can be deleted.
 *                         It may trigger log rolling if the active segment is deleted.
 *                       - Earliest offset of the log in response to ListOffsetRequest. To avoid OffsetOutOfRange exception after user seeks to earliest offset,
 *                         we make sure that logStartOffset <= log's highWatermark
 *                       Other activities such as log cleaning are not affected by logStartOffset.
 * @param recoveryPoint The offset at which to begin recovery--i.e. the first offset which has not been flushed to disk
 * @param scheduler The thread pool scheduler used for background actions
 * @param brokerTopicStats Container for Broker Topic Yammer Metrics
 * @param time The time instance used for checking the clock
 * @param maxProducerIdExpirationMs The maximum amount of time to wait before a producer id is considered expired
 * @param producerIdExpirationCheckIntervalMs How often to check for producer ids which need to be expired
 *
 * 以追加方式存储消息的日志类.
 *
 * 日志是由日志段的序列组成的, 每个日志段都带有一个基准位移, 该基准位移指向该段的第一条消息.
 *
 * 新的日志段会依据配置的规则(不超过特定大小或者不超过特定时间间隔)来生成.
 *
 * 参数 dir: 日志段所在的目录.
 * 参数 config: 日志配置属性.
 * 参数 logStartOffset: 可以暴露给kafka客户端的最老位移. logStartOffset在以下三种情况下会更新:
 *                        - 用户发起DeleteRecordsRequest请求
 *                        - broker日志保留机制
 *                        - broker日志截断机制
 *                      logStartOffset主要作用于以下两点:
 *                        - 日志删除. nextOffset小于或等于logStartOffset的日志段可以被删除而没有任何影响.
 *                          如果当前正在使用的日志段被删除了, 那么会触发一次日志滚动.
 *                        - 在ListOffsetRequest请求中返回此位移. 为了避免offsetOutOfRange异常(用户获取更老的位移导致),
 *                          需要保证logStartOffset <= 日志的高水位线 (TODO)
 * 参数 recoveryPoint: 故障恢复的起始位移, 也就是还没有刷到磁盘的第一条消息位移.
 * 参数 scheduler 用来执行后台任务的调度线程池.
 * 参数 brokerTopicStats: 用来存放broker的topic统计指标的容器.
 * 参数 time: 用来获取时间的实例.
 * 参数 maxProducerIdExpirationMs: 一个producer id超时的最大等待时间.
 * 参数 producerIdExpirationCheckIntervalMs: 隔多久检查一次producer id是否超时
 * 参数 topicPartition: 主题和分区信息
 * 参数 producerStateManager: 维护Producer最后写入的消息元数据
 * 参数 logDirFailureChannel: 日志目录下线的发布及订阅通道
 *
 */
@threadsafe
class Log(@volatile var dir: File,
          @volatile var config: LogConfig,
          @volatile var logStartOffset: Long,
          @volatile var recoveryPoint: Long,
          scheduler: Scheduler,
          brokerTopicStats: BrokerTopicStats,
          val time: Time,
          val maxProducerIdExpirationMs: Int,
          val producerIdExpirationCheckIntervalMs: Int,
          val topicPartition: TopicPartition,
          val producerStateManager: ProducerStateManager,
          logDirFailureChannel: LogDirFailureChannel) extends Logging with KafkaMetricsGroup {

  import kafka.log.Log._

  this.logIdent = s"[Log partition=$topicPartition, dir=${dir.getParent}] "

  /* A lock that guards all modifications to the log */
  //同步此日志所有更新操作的锁
  private val lock = new Object

  // The memory mapped buffer for index files of this log will be closed for index files of this log will be closed with either delete() or closeHandlers()
  // After memory mapped buffer is closed, no disk IO operation should be performed for this log
  // 当调用delete()方法或者closeHandlers()方法时, 此Log的索引文件的内存映射缓冲区会被关闭, 此后将禁止所有的磁盘IO操作
  @volatile private var isMemoryMappedBufferClosed = false

  /* last time it was flushed */
  // 最后刷新磁盘的时间
  private val lastFlushedTime = new AtomicLong(time.milliseconds)

  // 指定日志段文件的初始化大小.
  def initFileSize: Int = {
    if (config.preallocate)
      config.segmentSize
    else
      0
  }

  // 更新日志配置
  def updateConfig(updatedKeys: Set[String], newConfig: LogConfig): Unit = {
    //如果 日志保留时间 小于或等于 消息最大到达时间, 那么可能会导致日志频繁滚动
    if ((updatedKeys.contains(LogConfig.RetentionMsProp)
      || updatedKeys.contains(LogConfig.MessageTimestampDifferenceMaxMsProp))
      && topicPartition.partition == 0  // generate warnings only for one partition of each topic
      && newConfig.retentionMs < newConfig.messageTimestampDifferenceMaxMs)
      warn(s"${LogConfig.RetentionMsProp} for topic ${topicPartition.topic} is set to ${newConfig.retentionMs}. It is smaller than " +
        s"${LogConfig.MessageTimestampDifferenceMaxMsProp}'s value ${newConfig.messageTimestampDifferenceMaxMs}. " +
        s"This may result in frequent log rolling.")

    //更新
    this.config = newConfig
  }

  //检查内存映射缓冲区是否关闭
  private def checkIfMemoryMappedBufferClosed(): Unit = {
    if (isMemoryMappedBufferClosed)
      throw new KafkaStorageException(s"The memory mapped buffer for log of $topicPartition is already closed")
  }

  //下一条消息的位移元数据
  //(注: 由于读取不会加锁, Kafka使用此字段来解决更新与读取的线程同步问题, 详见read方法)
  @volatile private var nextOffsetMetadata: LogOffsetMetadata = _

  /* The earliest offset which is part of an incomplete transaction. This is used to compute the
   * last stable offset (LSO) in ReplicaManager. Note that it is possible that the "true" first unstable offset
   * gets removed from the log (through record or segment deletion). In this case, the first unstable offset
   * will point to the log start offset, which may actually be either part of a completed transaction or not
   * part of a transaction at all. However, since we only use the LSO for the purpose of restricting the
   * read_committed consumer to fetching decided data (i.e. committed, aborted, or non-transactional), this
   * temporary abuse seems justifiable and saves us from scanning the log after deletion to find the first offsets
   * of each ongoing transaction in order to compute a new first unstable offset. It is possible, however,
   * that this could result in disagreement between replicas depending on when they began replicating the log.
   * In the worst case, the LSO could be seen by a consumer to go backwards.
   */
  //第一条unstable消息的位移, 指向未完成事务的位移或者指向未复制事务的位移. 注意, 在老日志删除的情况下,
  //此值可能会指向日志最初位移, 但这并无大碍, 因为此值是用来计算最新stable消息位移(LSO)的
  @volatile var firstUnstableOffset: Option[LogOffsetMetadata] = None

  /* Keep track of the current high watermark in order to ensure that segments containing offsets at or above it are
   * not eligible for deletion. This means that the active segment is only eligible for deletion if the high watermark
   * equals the log end offset (which may never happen for a partition under consistent load). This is needed to
   * prevent the log start offset (which is exposed in fetch responses) from getting ahead of the high watermark.
   */
  //已完成复制的事务位移
  @volatile private var replicaHighWatermark: Option[Long] = None

  /* the actual segments of the log */
  // 日志段基准位移 -> 日志段(主要用来读取消息)
  private val segments: ConcurrentNavigableMap[java.lang.Long, LogSegment] = new ConcurrentSkipListMap[java.lang.Long, LogSegment]

  //leader的epoch -> 该epoch的初始位移
  @volatile private var _leaderEpochCache: LeaderEpochCache = initializeLeaderEpochCache()

  //初始化日志
  locally {
    val startMs = time.milliseconds

    //加载日志段文件, 并获取下一个消息位移
    val nextOffset = loadSegments()

    /* Calculate the offset of the next message */
    //生成下一个消息的位移元数据(消息位移, 日志段基准位移, 日志段内物理偏移)
    nextOffsetMetadata = new LogOffsetMetadata(nextOffset, activeSegment.baseOffset, activeSegment.size)

    //清除高于nextOffsetMetadata的leader epoch, 并持久化合法的leader epoch
    _leaderEpochCache.clearAndFlushLatest(nextOffsetMetadata.messageOffset)

    //根据日志段文件校准logStartOffset
    logStartOffset = math.max(logStartOffset, segments.firstEntry.getValue.baseOffset)

    // The earliest leader epoch may not be flushed during a hard failure. Recover it here.
    //清除更早的leader epoch数据
    _leaderEpochCache.clearAndFlushEarliest(logStartOffset)

    //加载生产者状态
    loadProducerState(logEndOffset, reloadFromCleanShutdown = hasCleanShutdownFile)

    info(s"Completed load of log with ${segments.size} segments, log start offset $logStartOffset and " +
      s"log end offset $logEndOffset in ${time.milliseconds() - startMs} ms")
  }

  private val tags = {
    val maybeFutureTag = if (isFuture) Map("is-future" -> "true") else Map.empty[String, String]
    Map("topic" -> topicPartition.topic, "partition" -> topicPartition.partition.toString) ++ maybeFutureTag
  }

  //日志段个数指标统计
  newGauge("NumLogSegments",
    new Gauge[Int] {
      def value = numberOfSegments
    },
    tags)

  //日志最老位移指标统计
  newGauge("LogStartOffset",
    new Gauge[Long] {
      def value = logStartOffset
    },
    tags)

  //日志下一条消息位移指标统计
  newGauge("LogEndOffset",
    new Gauge[Long] {
      def value = logEndOffset
    },
    tags)

  //日志大小指标统计
  newGauge("Size",
    new Gauge[Long] {
      def value = size
    },
    tags)

  //生产者过期检查的定时任务, 延迟producerIdExpirationCheckIntervalMs毫秒开始执行,
  //并且每隔producerIdExpirationCheckIntervalMs毫秒执行一次
  scheduler.schedule(name = "PeriodicProducerExpirationCheck", fun = () => {
    lock synchronized {
      producerStateManager.removeExpiredProducers(time.milliseconds)
    }
  }, period = producerIdExpirationCheckIntervalMs, delay = producerIdExpirationCheckIntervalMs, unit = TimeUnit.MILLISECONDS)


  /** The name of this log */
  //此日志所在的目录名称
  def name  = dir.getName()

  //leader的epoch -> 该epoch的初始位移
  def leaderEpochCache = _leaderEpochCache

  //初始化 (leader epoch -> 该epoch的初始位移) 这个缓存结构
  private def initializeLeaderEpochCache(): LeaderEpochCache = {
    // create the log directory if it doesn't exist
    Files.createDirectories(dir.toPath)
    new LeaderEpochFileCache(topicPartition, () => logEndOffsetMetadata,
      new LeaderEpochCheckpointFile(LeaderEpochFile.newFile(dir), logDirFailureChannel))
  }

  /**
   * Removes any temporary files found in log directory, and creates a list of all .swap files which could be swapped
   * in place of existing segment(s). For log splitting, we know that any .swap file whose base offset is higher than
   * the smallest offset .clean file could be part of an incomplete split operation. Such .swap files are also deleted
   * by this method.
   * @return Set of .swap files that are valid to be swapped in as segment files
   *
   * 删除临时文件, 返回目录中的日志段.swap文件列表. 另外在日志切割过程中(详见splitOverflowedSegment), 如果中途broker关闭, 那么会导致
   * 中间的.swap文件的位移比.cleaned文件的位移高, 对于这样的.swap文件也会删除
   */
  private def removeTempFilesAndCollectSwapFiles(): Set[File] = {

    //删除位移索引\时间索引\事务索引
    def deleteIndicesIfExist(baseFile: File, suffix: String = ""): Unit = {
      info(s"Deleting index files with suffix $suffix for baseFile $baseFile")
      val offset = offsetFromFile(baseFile)
      //删除位移索引
      Files.deleteIfExists(Log.offsetIndexFile(dir, offset, suffix).toPath)
      //删除时间索引
      Files.deleteIfExists(Log.timeIndexFile(dir, offset, suffix).toPath)
      //删除事务索引
      Files.deleteIfExists(Log.transactionIndexFile(dir, offset, suffix).toPath)
    }

    var swapFiles = Set[File]()
    var cleanFiles = Set[File]()
    var minCleanedFileOffset = Long.MaxValue

    //递归处理dir中的文件
    for (file <- dir.listFiles if file.isFile) {
      if (!file.canRead)
        throw new IOException(s"Could not read file $file")
      val filename = file.getName
      if (filename.endsWith(DeletedFileSuffix)) {
        //删除临时文件
        debug(s"Deleting stray temporary file ${file.getAbsolutePath}")
        Files.deleteIfExists(file.toPath)
      } else if (filename.endsWith(CleanedFileSuffix)) {
        //
        minCleanedFileOffset = Math.min(offsetFromFileName(filename), minCleanedFileOffset)
        cleanFiles += file
      } else if (filename.endsWith(SwapFileSuffix)) {
        // we crashed in the middle of a swap operation, to recover:
        // if a log, delete the index files, complete the swap operation later
        // if an index just delete the index files, they will be rebuilt
        //
        // 如果发现.swap文件, 那么存在两种情况:
        // 1).swap文件为日志文件: 删除其索引文件, 后面会重建索引;
        // 2).swap文件为索引文件: 直接删除.
        val baseFile = new File(CoreUtils.replaceSuffix(file.getPath, SwapFileSuffix, ""))
        info(s"Found file ${file.getAbsolutePath} from interrupted swap operation.")
        if (isIndexFile(baseFile)) {
          deleteIndicesIfExist(baseFile)
        } else if (isLogFile(baseFile)) {
          deleteIndicesIfExist(baseFile)
          swapFiles += file
        }
      }
    }

    // KAFKA-6264: Delete all .swap files whose base offset is greater than the minimum .cleaned segment offset. Such .swap
    // files could be part of an incomplete split operation that could not complete. See Log#splitOverflowedSegment
    // for more details about the split operation.
    //
    // 对于那些位移比最小的.cleaned文件的位移还要小的.swap文件, 这些文件是由于不完整文件切割导致的(详见Log#splitOverflowedSegment),
    // 这里也进行删除
    val (invalidSwapFiles, validSwapFiles) = swapFiles.partition(file => offsetFromFile(file) >= minCleanedFileOffset)
    invalidSwapFiles.foreach { file =>
      debug(s"Deleting invalid swap file ${file.getAbsoluteFile} minCleanedFileOffset: $minCleanedFileOffset")
      val baseFile = new File(CoreUtils.replaceSuffix(file.getPath, SwapFileSuffix, ""))
      deleteIndicesIfExist(baseFile, SwapFileSuffix)
      Files.deleteIfExists(file.toPath)
    }

    // Now that we have deleted all .swap files that constitute an incomplete split operation, let's delete all .clean files
    // 清理所有的.cleaned文件
    cleanFiles.foreach { file =>
      debug(s"Deleting stray .clean file ${file.getAbsolutePath}")
      Files.deleteIfExists(file.toPath)
    }

    validSwapFiles
  }

  /**
   * This method does not need to convert IOException to KafkaStorageException because it is only called before all logs are loaded
   * It is possible that we encounter a segment with index offset overflow in which case the LogSegmentOffsetOverflowException
   * will be thrown. Note that any segments that were opened before we encountered the exception will remain open and the
   * caller is responsible for closing them appropriately, if needed.
   * @throws LogSegmentOffsetOverflowException if the log directory contains a segment with messages that overflow the index offset
   *
   * 加载日志段文件, 如果遇到位移溢出的日志段, 那么会抛出LogSegmentOffsetOverflowException异常, 调用者需要关闭所有的日志段文件, 并重新加载
   */
  private def loadSegmentFiles(): Unit = {
    // load segments in ascending order because transactional data from one segment may depend on the
    // segments that come before it
    // 从小到大加载, 因为事务消息可能会横跨两个日志段
    for (file <- dir.listFiles.sortBy(_.getName) if file.isFile) {
      if (isIndexFile(file)) {
        // if it is an index file, make sure it has a corresponding .log file
        // 如果为索引文件, 那么需要保证有相应的.log文件
        val offset = offsetFromFile(file)
        val logFile = Log.logFile(dir, offset)
        if (!logFile.exists) {
          warn(s"Found an orphaned index file ${file.getAbsolutePath}, with no corresponding log file.")
          Files.deleteIfExists(file.toPath)
        }
      } else if (isLogFile(file)) {
        // if it's a log file, load the corresponding log segment
        // 加载日志段
        val baseOffset = offsetFromFile(file)
        val timeIndexFileNewlyCreated = !Log.timeIndexFile(dir, baseOffset).exists()
        val segment = LogSegment.open(dir = dir,
          baseOffset = baseOffset,
          config,
          time = time,
          fileAlreadyExists = true)

        //如果索引文件不存在或者损坏, 那么重新恢复日志段
        try segment.sanityCheck(timeIndexFileNewlyCreated)
        catch {
          case _: NoSuchFileException =>
            error(s"Could not find offset index file corresponding to log file ${segment.log.file.getAbsolutePath}, " +
              "recovering segment and rebuilding index files...")
            recoverSegment(segment)
          case e: CorruptIndexException =>
            warn(s"Found a corrupted index file corresponding to log file ${segment.log.file.getAbsolutePath} due " +
              s"to ${e.getMessage}}, recovering segment and rebuilding index files...")
            recoverSegment(segment)
        }
        //在内存中记录该日志段
        addSegment(segment)
      }
    }
  }

  /**
   * Recover the given segment.
   * @param segment Segment to recover
   * @param leaderEpochCache Optional cache for updating the leader epoch during recovery
   * @return The number of bytes truncated from the segment
   * @throws LogSegmentOffsetOverflowException if the segment contains messages that cause index offset overflow
   *
   * 恢复日志段
   * 参数 segment: 要恢复的日志段
   * 参数 leaderEpochCache: 可选参数, 恢复期间用来更新leader epoch的缓存
   * 返回值 日志段被截断的字节数
   */
  private def recoverSegment(segment: LogSegment, leaderEpochCache: Option[LeaderEpochCache] = None): Int = lock synchronized {
    //记录生产者状态
    val stateManager = new ProducerStateManager(topicPartition, dir, maxProducerIdExpirationMs)

    //截断生产者状态, 只保留从日志最老位移到此日志段基准位移之间的生产者信息
    stateManager.truncateAndReload(logStartOffset, segment.baseOffset, time.milliseconds)

    //从此segment之前的日志段中读取消息, 如果基准位移大于或等于stateManager中记录的生产者位移信息, 那么更新stateManager的生产者信息
    logSegments(stateManager.mapEndOffset, segment.baseOffset).foreach { segment =>
      val startOffset = math.max(segment.baseOffset, stateManager.mapEndOffset)
      val fetchDataInfo = segment.read(startOffset, None, Int.MaxValue)
      if (fetchDataInfo != null)
        loadProducersFromLog(stateManager, fetchDataInfo.records)
    }
    //更新stateManager的最大生产者消息位移
    stateManager.updateMapEndOffset(segment.baseOffset)

    // take a snapshot for the first recovered segment to avoid reloading all the segments if we shutdown before we
    // checkpoint the recovery point
    //记录生产者信息快照
    stateManager.takeSnapshot()
    //从此segment中恢复数据
    val bytesTruncated = segment.recover(stateManager, leaderEpochCache)

    // once we have recovered the segment's data, take a snapshot to ensure that we won't
    // need to reload the same segment again while recovering another segment.
    //完成segment恢复后, 再次记录快照
    stateManager.takeSnapshot()
    bytesTruncated
  }

  /**
   * This method does not need to convert IOException to KafkaStorageException because it is only called before all logs
   * are loaded.
   * @throws LogSegmentOffsetOverflowException if the swap file contains messages that cause the log segment offset to
   *                                           overflow. Note that this is currently a fatal exception as we do not have
   *                                           a way to deal with it. The exception is propagated all the way up to
   *                                           KafkaServer#startup which will cause the broker to shut down if we are in
   *                                           this situation. This is expected to be an extremely rare scenario in practice,
   *                                           and manual intervention might be required to get out of it.
   *
   * 完成.swap文件处理, swap文件中不应该包含位移溢出的消息, 如有则抛出LogSegmentOffsetOverflowException
   */
  private def completeSwapOperations(swapFiles: Set[File]): Unit = {
    for (swapFile <- swapFiles) {
      val logFile = new File(CoreUtils.replaceSuffix(swapFile.getPath, SwapFileSuffix, ""))
      val baseOffset = offsetFromFile(logFile)
      val swapSegment = LogSegment.open(swapFile.getParentFile,
        baseOffset = baseOffset,
        config,
        time = time,
        fileSuffix = SwapFileSuffix)
      info(s"Found log file ${swapFile.getPath} from interrupted swap operation, repairing.")
      //恢复日志段
      recoverSegment(swapSegment)

      // We create swap files for two cases:
      // (1) Log cleaning where multiple segments are merged into one, and
      // (2) Log splitting where one segment is split into multiple.
      //
      // Both of these mean that the resultant swap segments be composed of the original set, i.e. the swap segment
      // must fall within the range of existing segment(s). If we cannot find such a segment, it means the deletion
      // of that segment was successful. In such an event, we should simply rename the .swap to .log without having to
      // do a replace with an existing segment.
      //
      // swap文件可能由于如下两种情况产生:
      // 1) 日志清理, 多个日志段合并成一个;
      // 2) 日志切割, 一个日志段分成多个日志段;
      // 无论属于哪种情况, 新的日志段都是由老的日志段产生的, 如果能找到老的日志段, 那么将其删除, 如果未找到则证明已经删除了
      val oldSegments = logSegments(swapSegment.baseOffset, swapSegment.readNextOffset).filter { segment =>
        segment.readNextOffset > swapSegment.baseOffset
      }
      // 使用新的日志段替换老的日志段, 并异步删除老的日志段
      replaceSegments(Seq(swapSegment), oldSegments.toSeq, isRecoveredSwapFile = true)
    }
  }

  /**
   * Load the log segments from the log files on disk and return the next offset.
   * This method does not need to convert IOException to KafkaStorageException because it is only called before all logs
   * are loaded.
   * @throws LogSegmentOffsetOverflowException if we encounter a .swap file with messages that overflow index offset; or when
   *                                           we find an unexpected number of .log files with overflow
   *
   * 加载日志段, 并返回下一个消息的位移. 如果遇到位移溢出的.swap文件时或者溢出的.log文件数目有误时, 抛出LogSegmentOffsetOverflowException
   */
  private def loadSegments(): Long = {
    // first do a pass through the files in the log directory and remove any temporary files
    // and find any interrupted swap operations

    // 删除临时文件, 并返回有效的.swap文件列表. 这里的"有效"是指, 已经完成文件切割所形成的.swap文件; 而无效的.swap文件则是由不完整文件切割
    // 所输出的文件, 详见splitOverflowedSegment方法
    val swapFiles = removeTempFilesAndCollectSwapFiles()

    // Now do a second pass and load all the log and index files.
    // We might encounter legacy log segments with offset overflow (KAFKA-6264). We need to split such segments. When
    // this happens, restart loading segment files from scratch.
    //
    //如果发现日志段中的消息位移大于{{base_offset + Int.MaxValue}} (详见KAFKA-6264), 那么切割这样的日志段并重试
    retryOnOffsetOverflow {
      // In case we encounter a segment with offset overflow, the retry logic will split it after which we need to retry
      // loading of segments. In that case, we also need to close all segments that could have been left open in previous
      // call to loadSegmentFiles().
      //
      // 当加载过程中遇到日志段位移溢出时, retryOnOffsetOverflow方法中会进行日志切割, 切割日志段之后, 需要关闭和重新加载
      logSegments.foreach(_.close())
      segments.clear()
      loadSegmentFiles()
    }

    // Finally, complete any interrupted swap operations. To be crash-safe,
    // log files that are replaced by the swap segment should be renamed to .deleted
    // before the swap file is restored as the new segment file.
    //
    //完成swap操作, 在把swap文件存储为新的日志段文件前, 需要把被替代的旧日志段文件重命名成.deleted
    completeSwapOperations(swapFiles)

    if (logSegments.isEmpty) {
      // no existing segments, create a new mutable segment beginning at offset 0
      // 未发现日志段, 那么新建一个日志段, 位移从0开始
      addSegment(LogSegment.open(dir = dir,
        baseOffset = 0,
        config,
        time = time,
        fileAlreadyExists = false,
        initFileSize = this.initFileSize,
        preallocate = config.preallocate))
      0
    } else if (!dir.getAbsolutePath.endsWith(Log.DeleteDirSuffix)) {
      //从日志段文件中恢复
      val nextOffset = retryOnOffsetOverflow {
        recoverLog()
      }

      // reset the index size of the currently active log segment to allow more entries
      // 重置当前日志段的索引大小, 以便写入更多的消息
      activeSegment.resizeIndexes(config.maxIndexSize)
      nextOffset
    } else 0
  }

  /**
   * 更新日志结束位移(也就是下一条消息位移)
   * @param messageOffset
   */
  private def updateLogEndOffset(messageOffset: Long) {
    nextOffsetMetadata = new LogOffsetMetadata(messageOffset, activeSegment.baseOffset, activeSegment.size)
  }

  /**
   * Recover the log segments and return the next offset after recovery.
   * This method does not need to convert IOException to KafkaStorageException because it is only called before all
   * logs are loaded.
   * @throws LogSegmentOffsetOverflowException if we encountered a legacy segment with offset overflow
   *
   * 从日志中恢复, 并返回恢复后的下一条消息位移
   */
  private def recoverLog(): Long = {
    // if we have the clean shutdown marker, skip recovery
    // 如果正在关闭过程中, 则跳过恢复过程
    if (!hasCleanShutdownFile) {
      // okay we need to actually recover this log
      // 这里开始恢复
      val unflushed = logSegments(this.recoveryPoint, Long.MaxValue).iterator
      while (unflushed.hasNext) {
        val segment = unflushed.next
        info(s"Recovering unflushed segment ${segment.baseOffset}")
        val truncatedBytes =
          try {
            //恢复日志段
            recoverSegment(segment, Some(_leaderEpochCache))
          } catch {
            case _: InvalidOffsetException =>
              //如果发现该日志段中的消息位移损坏, 那么删除该日志段
              val startOffset = segment.baseOffset
              warn("Found invalid offset during recovery. Deleting the corrupt segment and " +
                s"creating an empty one with starting offset $startOffset")
              segment.truncateTo(startOffset)
          }
        if (truncatedBytes > 0) {
          // we had an invalid message, delete all remaining log
          // 如果该日志段损坏, 那么删除其后的所有日志段
          warn(s"Corruption found in segment ${segment.baseOffset}, truncating to offset ${segment.readNextOffset}")
          unflushed.foreach(deleteSegment)
        }
      }
    }
    recoveryPoint = activeSegment.readNextOffset
    recoveryPoint
  }

  /**
   * 加载生产者的状态
   * @param lastOffset: 最大的消息位移
   * @param reloadFromCleanShutdown: 是否为优雅退出后加载
   */
  private def loadProducerState(lastOffset: Long, reloadFromCleanShutdown: Boolean): Unit = lock synchronized {
    checkIfMemoryMappedBufferClosed()
    //消息格式版本
    val messageFormatVersion = config.messageFormatVersion.recordVersion.value
    info(s"Loading producer state from offset $lastOffset with message format version $messageFormatVersion")

    // We want to avoid unnecessary scanning of the log to build the producer state when the broker is being
    // upgraded. The basic idea is to use the absence of producer snapshot files to detect the upgrade case,
    // but we have to be careful not to assume too much in the presence of broker failures. The two most common
    // upgrade cases in which we expect to find no snapshots are the following:
    //
    // 1. The broker has been upgraded, but the topic is still on the old message format.
    // 2. The broker has been upgraded, the topic is on the new message format, and we had a clean shutdown.
    //
    // If we hit either of these cases, we skip producer state loading and write a new snapshot at the log end
    // offset (see below). The next time the log is reloaded, we will load producer state using this snapshot
    // (or later snapshots). Otherwise, if there is no snapshot file, then we have to rebuild producer state
    // from the first segment.
    //
    // 为了避免由于broker升级而导致扫描日志来重建生产者状态, 这里根据是否生产者快照文件是否存在来检测是否为升级的场景.
    // 通常会由于如下两种情况而导致没有快照文件:
    //    1. broker升级, 但主题仍然是老的消息格式;
    //    2. broker升级, 且主题是新的消息格式, 并且broker是优雅退出的.
    // 如果为以上情况之一, 那么可以跳过加载生产者状态, 并且在日志的结尾位移处记录快照. 这样下一次加载日志时, 可以使用这个快照;
    // 如果不为以上情况, 而且没有快照文件, 那么需要从第一个日志段开始重建生产者状态
    if (producerStateManager.latestSnapshotOffset.isEmpty && (messageFormatVersion < RecordBatch.MAGIC_VALUE_V2 || reloadFromCleanShutdown)) {
      // To avoid an expensive scan through all of the segments, we take empty snapshots from the start of the
      // last two segments and the last offset. This should avoid the full scan in the case that the log needs
      // truncation.
      //
      // 为了避免扫描全量日志段, 这里在最后两个日志段的起始处以及最后的消息位移处记录空快照, 这样在日志需要截断时可以避免全量扫描
      // (这里可以这么理解, 如果broker退出是由于判断条件之一的话, 那么可以忽略之前的生产者状态, 这里记录的空快照也就是清理之前
      //  的生产者状态)
      val nextLatestSegmentBaseOffset = lowerSegment(activeSegment.baseOffset).map(_.baseOffset)
      val offsetsToSnapshot = Seq(nextLatestSegmentBaseOffset, Some(activeSegment.baseOffset), Some(lastOffset))
      offsetsToSnapshot.flatten.foreach { offset =>
        producerStateManager.updateMapEndOffset(offset)
        producerStateManager.takeSnapshot()
      }
    } else {
      val isEmptyBeforeTruncation = producerStateManager.isEmpty && producerStateManager.mapEndOffset >= lastOffset
      producerStateManager.truncateAndReload(logStartOffset, lastOffset, time.milliseconds())

      // Only do the potentially expensive reloading if the last snapshot offset is lower than the log end
      // offset (which would be the case on first startup) and there were active producers prior to truncation
      // (which could be the case if truncating after initial loading). If there weren't, then truncating
      // shouldn't change that fact (although it could cause a producerId to expire earlier than expected),
      // and we can skip the loading. This is an optimization for users which are not yet using
      // idempotent/transactional features yet.
      //
      // 只有最新的快照文件的消息位移比最大的消息位移小时(在第一次启动)而且在日志截断前存在活跃的生产者, 才进行重建生产者状态;
      // 如果不满足以上任一条件, 那么截断不会对生产者状态有影响(除了导致一个生产者比预期更早过期之外), 我们可以避免重建状态.
      if (lastOffset > producerStateManager.mapEndOffset && !isEmptyBeforeTruncation) {
        logSegments(producerStateManager.mapEndOffset, lastOffset).foreach { segment =>
          val startOffset = Utils.max(segment.baseOffset, producerStateManager.mapEndOffset, logStartOffset)
          producerStateManager.updateMapEndOffset(startOffset)
          producerStateManager.takeSnapshot()

          val fetchDataInfo = segment.read(startOffset, Some(lastOffset), Int.MaxValue)
          if (fetchDataInfo != null)
            loadProducersFromLog(producerStateManager, fetchDataInfo.records)
        }
      }

      producerStateManager.updateMapEndOffset(lastOffset)
      updateFirstUnstableOffset()
    }
  }

  /**
   * 从日志中恢复生产者状态
   * @param producerStateManager
   * @param records
   */
  private def loadProducersFromLog(producerStateManager: ProducerStateManager, records: Records): Unit = {
    val loadedProducers = mutable.Map.empty[Long, ProducerAppendInfo]
    val completedTxns = ListBuffer.empty[CompletedTxn]
    records.batches.asScala.foreach { batch =>
      if (batch.hasProducerId) {
        val maybeCompletedTxn = updateProducers(batch, loadedProducers, isFromClient = false)
        maybeCompletedTxn.foreach(completedTxns += _)
      }
    }
    loadedProducers.values.foreach(producerStateManager.update)
    completedTxns.foreach(producerStateManager.completeTxn)
  }

  private[log] def activeProducersWithLastSequence: Map[Long, Int] = lock synchronized {
    producerStateManager.activeProducers.map { case (producerId, producerIdEntry) =>
      (producerId, producerIdEntry.lastSeq)
    }
  }

  /**
   * Check if we have the "clean shutdown" file
   *
   * 检查是否存在优雅关闭的标识文件
   */
  private def hasCleanShutdownFile: Boolean = new File(dir.getParentFile, CleanShutdownFile).exists()

  /**
   * The number of segments in the log.
   * Take care! this is an O(n) operation.
   *
   * 日志段的个数
   */
  def numberOfSegments: Int = segments.size

  /**
   * Close this log.
   * The memory mapped buffer for index files of this log will be left open until the log is deleted.
   *
   * 关闭日志. 索引文件的内存映射仍然保持打开状态, 直到此日志被删除.
   */
  def close() {
    debug("Closing log")
    lock synchronized {
      checkIfMemoryMappedBufferClosed()
      maybeHandleIOException(s"Error while renaming dir for $topicPartition in dir ${dir.getParent}") {
        // We take a snapshot at the last written offset to hopefully avoid the need to scan the log
        // after restarting and to ensure that we cannot inadvertently hit the upgrade optimization
        // (the clean shutdown file is written after the logs are all closed).
        //
        // 在最后的写入位移处记录快照以避免重启后需要扫描日志, 并且保证不与升级优化产生冲突.
        // (另外, 在所有日志文件都关闭后, 才会写入优雅关闭标识文件)
        producerStateManager.takeSnapshot()
        logSegments.foreach(_.close())
      }
    }
  }

  /**
   * Rename the directory of the log
   *
   * @throws KafkaStorageException if rename fails
   *
   * 重命名此日志的目录
   */
  def renameDir(name: String) {
    lock synchronized {
      maybeHandleIOException(s"Error while renaming dir for $topicPartition in log dir ${dir.getParent}") {
        val renamedDir = new File(dir.getParent, name)
        Utils.atomicMoveWithFallback(dir.toPath, renamedDir.toPath)
        if (renamedDir != dir) {
          dir = renamedDir
          //更新日志段的目录
          logSegments.foreach(_.updateDir(renamedDir))
          //更新维护生产者状态的目录
          producerStateManager.logDir = dir
          // re-initialize leader epoch cache so that LeaderEpochCheckpointFile.checkpoint can correctly reference
          // the checkpoint file in renamed log directory
          // 重新初始化leader epoch缓存, 这样在新的目录下生成检查点文件
          _leaderEpochCache = initializeLeaderEpochCache()
        }
      }
    }
  }

  /**
   * Close file handlers used by log but don't write to disk. This is called if the log directory is offline
   *
   * 关闭日志的文件处理但不写入磁盘, 此方法在日志目录下线时执行
   */
  def closeHandlers() {
    debug("Closing handlers")
    lock synchronized {
      logSegments.foreach(_.closeHandlers())
      isMemoryMappedBufferClosed = true
    }
  }

  /**
   * Append this message set to the active segment of the log, assigning offsets and Partition Leader Epochs
   *
   * @param records The records to append
   * @param isFromClient Whether or not this append is from a producer
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @return Information about the appended messages including the first and last offset.
   *
   * 追加消息到此日志的活跃日志段中, 并设置位移和分区leader epoch信息
   *
   * 参数 records: 追加的消息
   * 参数 isFromClient: 是否来源于一个生产者
   * 抛出异常 KafkaStorageException: 如果写入发生IO异常
   * 返回 追加第一条和最后一条消息位移的信息
   */
  def appendAsLeader(records: MemoryRecords, leaderEpoch: Int, isFromClient: Boolean = true): LogAppendInfo = {
    append(records, isFromClient, assignOffsets = true, leaderEpoch)
  }

  /**
   * Append this message set to the active segment of the log without assigning offsets or Partition Leader Epochs
   *
   * @param records The records to append
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @return Information about the appended messages including the first and last offset.
   */
  def appendAsFollower(records: MemoryRecords): LogAppendInfo = {
    append(records, isFromClient = false, assignOffsets = false, leaderEpoch = -1)
  }

  /**
   * Append this message set to the active segment of the log, rolling over to a fresh segment if necessary.
   *
   * This method will generally be responsible for assigning offsets to the messages,
   * however if the assignOffsets=false flag is passed we will only check that the existing offsets are valid.
   *
   * @param records The log records to append
   * @param isFromClient Whether or not this append is from a producer
   * @param assignOffsets Should the log assign offsets to this message set or blindly apply what it is given
   * @param leaderEpoch The partition's leader epoch which will be applied to messages when offsets are assigned on the leader
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @throws OffsetsOutOfOrderException If out of order offsets found in 'records'
   * @throws UnexpectedAppendOffsetException If the first or last offset in append is less than next offset
   * @return Information about the appended messages including the first and last offset.
   *
   * 追加消息到当前的日志段中, 可能会触发日志滚动并生成新的日志段. 此方法通常负责设置消息的位移, 不过当assignOffsets=false时,
   * 只会检查已经存在的位移是否合法.
   *
   * 参数 records: 需要追加的日志记录
   * 参数 isFromClient: 是否来源于生产者
   * 参数 assignOffsets: 是否需要设置消息的位移, 还是直接使用当前消息中已经存在的位移
   * 参数 leaderEpoch: 当leader设置消息位移的同时设置的epoch值
   * 抛出KafkaStorageException: 当发生IO异常时抛出
   * 抛出OffsetsOutOfOrderException: 如果发现记录中的位移乱序
   * 抛出UnexpectedAppendOffsetException: 如果记录中的第一条或最后一条消息的位移小于下一条消息位移
   * 返回 追加的消息信息(包括第一条和最后一条消息位移)
   */
  private def append(records: MemoryRecords, isFromClient: Boolean, assignOffsets: Boolean, leaderEpoch: Int): LogAppendInfo = {
    maybeHandleIOException(s"Error while appending records to $topicPartition in dir ${dir.getParent}") {
      //校验消息, 并获取追加的消息位移等信息
      val appendInfo = analyzeAndValidateRecords(records, isFromClient = isFromClient)

      // return if we have no valid messages or if this is a duplicate of the last appended entry
      // 如果没有合法的消息或者消息重复, 那么直接返回
      if (appendInfo.shallowCount == 0)
        return appendInfo

      // trim any invalid bytes or partial messages before appending it to the on-disk log
      // 去除冗余字节或者不完整的消息
      var validRecords = trimInvalidBytes(records, appendInfo)

      // they are valid, insert them in the log
      // 追加到日志中
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        if (assignOffsets) {
          // assign offsets to the message set
          // 下面设置消息的位移
          val offset = new LongRef(nextOffsetMetadata.messageOffset)
          appendInfo.firstOffset = Some(offset.value)
          val now = time.milliseconds
          val validateAndOffsetAssignResult = try {
            //校验消息, 并且设置位移
            LogValidator.validateMessagesAndAssignOffsets(validRecords,
              offset,
              time,
              now,
              appendInfo.sourceCodec,
              appendInfo.targetCodec,
              config.compact,
              config.messageFormatVersion.recordVersion.value,
              config.messageTimestampType,
              config.messageTimestampDifferenceMaxMs,
              leaderEpoch,
              isFromClient)
          } catch {
            case e: IOException =>
              throw new KafkaException(s"Error validating messages while appending to log $name", e)
          }
          //获取设置位移后的消息记录, 最大时间戳(及其位移), 最大消息位移, 以及
          validRecords = validateAndOffsetAssignResult.validatedRecords
          appendInfo.maxTimestamp = validateAndOffsetAssignResult.maxTimestamp
          appendInfo.offsetOfMaxTimestamp = validateAndOffsetAssignResult.shallowOffsetOfMaxTimestamp
          appendInfo.lastOffset = offset.value - 1
          appendInfo.recordConversionStats = validateAndOffsetAssignResult.recordConversionStats
          //如果时间戳设置为LogAppendTime, 那么设置其时间为现在
          if (config.messageTimestampType == TimestampType.LOG_APPEND_TIME)
            appendInfo.logAppendTime = now

          // re-validate message sizes if there's a possibility that they have changed (due to re-compression or message
          // format conversion)
          // 在(可能的)再压缩和消息格式转换后, 重新验证消息大小是否超过上限
          if (validateAndOffsetAssignResult.messageSizeMaybeChanged) {
            for (batch <- validRecords.batches.asScala) {
              if (batch.sizeInBytes > config.maxMessageSize) {
                // we record the original message set size instead of the trimmed size
                // to be consistent with pre-compression bytesRejectedRate recording
                brokerTopicStats.topicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
                brokerTopicStats.allTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
                throw new RecordTooLargeException(s"Message batch size is ${batch.sizeInBytes} bytes in append to" +
                  s"partition $topicPartition which exceeds the maximum configured size of ${config.maxMessageSize}.")
              }
            }
          }
        } else {
          // 直接使用消息中存在的位移

          if (!appendInfo.offsetsMonotonic)
            throw new OffsetsOutOfOrderException(s"Out of order offsets found in append to $topicPartition: " +
                                                 records.records.asScala.map(_.offset))

          // 如果消息中的第一个位移比nextOffsetMetadata小, 那么抛出UnexpectedAppendOffsetException异常
          if (appendInfo.firstOrLastOffsetOfFirstBatch < nextOffsetMetadata.messageOffset) {
            // we may still be able to recover if the log is empty
            // one example: fetching from log start offset on the leader which is not batch aligned,
            // which may happen as a result of AdminClient#deleteRecords()
            val firstOffset = appendInfo.firstOffset match {
              case Some(offset) => offset
              case None => records.batches.asScala.head.baseOffset()
            }

            val firstOrLast = if (appendInfo.firstOffset.isDefined) "First offset" else "Last offset of the first batch"
            throw new UnexpectedAppendOffsetException(
              s"Unexpected offset in append to $topicPartition. $firstOrLast " +
              s"${appendInfo.firstOrLastOffsetOfFirstBatch} is less than the next offset ${nextOffsetMetadata.messageOffset}. " +
              s"First 10 offsets in append: ${records.records.asScala.take(10).map(_.offset)}, last offset in" +
              s" append: ${appendInfo.lastOffset}. Log start offset = $logStartOffset",
              firstOffset, appendInfo.lastOffset)
          }
        }

        // update the epoch cache with the epoch stamped onto the message by the leader
        // 根据消息中设置的leader epoch, 更新epoch缓存
        validRecords.batches.asScala.foreach { batch =>
          if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
            _leaderEpochCache.assign(batch.partitionLeaderEpoch, batch.baseOffset)
        }

        // check messages set size may be exceed config.segmentSize
        // 如果记录大小超过了日志段, 那么抛出RecordBatchTooLargeException异常
        if (validRecords.sizeInBytes > config.segmentSize) {
          throw new RecordBatchTooLargeException(s"Message batch size is ${validRecords.sizeInBytes} bytes in append " +
            s"to partition $topicPartition, which exceeds the maximum configured segment size of ${config.segmentSize}.")
        }

        // now that we have valid records, offsets assigned, and timestamps updated, we need to
        // validate the idempotent/transactional state of the producers and collect some metadata
        // 现在已经验证了记录合法性, 设置了位移, 并且更新了时间戳, 现在验证生产者的事务状态并收集一些元信息
        val (updatedProducers, completedTxns, maybeDuplicate) = analyzeAndValidateProducerState(validRecords, isFromClient)

        //如果为重复消息, 那么使用之前记录的信息更新此次追加的appendInfo
        maybeDuplicate.foreach { duplicate =>
          appendInfo.firstOffset = Some(duplicate.firstOffset)
          appendInfo.lastOffset = duplicate.lastOffset
          appendInfo.logAppendTime = duplicate.timestamp
          appendInfo.logStartOffset = logStartOffset
          return appendInfo
        }

        // maybe roll the log if this segment is full
        // 根据写入的消息大小判断是否需要日志滚动, 如果需要则进行滚动并返回新的日志段; 否则使用原有的日志段
        val segment = maybeRoll(validRecords.sizeInBytes, appendInfo)

        //生成日志位移元数据
        val logOffsetMetadata = LogOffsetMetadata(
          messageOffset = appendInfo.firstOrLastOffsetOfFirstBatch,
          segmentBaseOffset = segment.baseOffset,
          relativePositionInSegment = segment.size)

        //日志段新增记录
        segment.append(largestOffset = appendInfo.lastOffset,
          largestTimestamp = appendInfo.maxTimestamp,
          shallowOffsetOfMaxTimestamp = appendInfo.offsetOfMaxTimestamp,
          records = validRecords)

        // update the producer state
        // 更新生产者状态
        for ((_, producerAppendInfo) <- updatedProducers) {
          producerAppendInfo.maybeCacheTxnFirstOffsetMetadata(logOffsetMetadata)
          producerStateManager.update(producerAppendInfo)
        }

        // update the transaction index with the true last stable offset. The last offset visible
        // to consumers using READ_COMMITTED will be limited by this value and the high watermark.
        // 使用stable位移更新事务索引. 对于使用READ_COMMITED级别的消费者, 最新的可见位移由此值与高水位线决定
        for (completedTxn <- completedTxns) {
          val lastStableOffset = producerStateManager.completeTxn(completedTxn)
          segment.updateTxnIndex(completedTxn, lastStableOffset)
        }

        // always update the last producer id map offset so that the snapshot reflects the current offset
        // even if there isn't any idempotent data being written
        // 更新生产者位移状态
        producerStateManager.updateMapEndOffset(appendInfo.lastOffset + 1)

        // increment the log end offset
        // 更新logEndOffset
        updateLogEndOffset(appendInfo.lastOffset + 1)

        // update the first unstable offset (which is used to compute LSO)
        //更新第一条unstable位移(用来计算最新的stable位移)
        updateFirstUnstableOffset()

        trace(s"Appended message set with last offset: ${appendInfo.lastOffset}, " +
          s"first offset: ${appendInfo.firstOffset}, " +
          s"next offset: ${nextOffsetMetadata.messageOffset}, " +
          s"and messages: $validRecords")

        //如果未刷盘消息量达到一定阈值, 那么刷盘
        if (unflushedMessages >= config.flushInterval)
          flush()

        appendInfo
      }
    }
  }

  /**
   * 更新高水位线
   * @param highWatermark
   */
  def onHighWatermarkIncremented(highWatermark: Long): Unit = {
    lock synchronized {
      //更新replicaHighWatermark
      replicaHighWatermark = Some(highWatermark)
      //更新生产者位移状态信息
      producerStateManager.onHighWatermarkUpdated(highWatermark)
      //更新第一条unstable消息位移信息
      updateFirstUnstableOffset()
    }
  }

  /**
   * 更新第一条unstable消息位移.
   *
   * 内部会根据producerStateManager来获取"未复制事务消息"和"未完成事务消息"中的位移最小者, 然后将其与日志起始位移比较取最大值,
   * 此值为第一条unstable消息.
   *
   */
  private def updateFirstUnstableOffset(): Unit = lock synchronized {
    checkIfMemoryMappedBufferClosed()

    //获取第一条unstable位移元数据
    val updatedFirstStableOffset = producerStateManager.firstUnstableOffset match {
      case Some(logOffsetMetadata) if logOffsetMetadata.messageOffsetOnly || logOffsetMetadata.messageOffset < logStartOffset =>
        //如果该logOffsetMetadata只包含消息位移信息, 那么从日志段中获取其他数据
        //如果消息数据太老(位移比logStartOffset小), 那么获取logStartOffset所对应的位移元数据
        val offset = math.max(logOffsetMetadata.messageOffset, logStartOffset)
        val segment = segments.floorEntry(offset).getValue
        val position  = segment.translateOffset(offset)
        Some(LogOffsetMetadata(offset, segment.baseOffset, position.position))
      case other => other
    }

    //更新第一条unstable位移
    if (updatedFirstStableOffset != this.firstUnstableOffset) {
      debug(s"First unstable offset updated to $updatedFirstStableOffset")
      this.firstUnstableOffset = updatedFirstStableOffset
    }
  }

  /**
   * Increment the log start offset if the provided offset is larger.
   * 更新logStartOffset.
   * 方法内部判断是否新的logStartOffset是否更大, 是则更新, 否则不做改动
   */
  def maybeIncrementLogStartOffset(newLogStartOffset: Long) {
    // We don't have to write the log start offset to log-start-offset-checkpoint immediately.
    // The deleteRecordsOffset may be lost only if all in-sync replicas of this broker are shutdown
    // in an unclean manner within log.flush.start.offset.checkpoint.interval.ms. The chance of this happening is low.
    maybeHandleIOException(s"Exception while increasing log start offset for $topicPartition to $newLogStartOffset in dir ${dir.getParent}") {
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        //只有新的logStartOffset更大才执行更新操作
        if (newLogStartOffset > logStartOffset) {
          info(s"Incrementing log start offset to $newLogStartOffset")
          //更新logStartOffset
          logStartOffset = newLogStartOffset

          //清除老的epoch信息并刷盘
          _leaderEpochCache.clearAndFlushEarliest(logStartOffset)

          //更新生产者状态信息
          producerStateManager.truncateHead(logStartOffset)

          //更新第一条unstable位移
          updateFirstUnstableOffset()
        }
      }
    }
  }

  /**
   * 校验生产者状态
   * @param records
   * @param isFromClient
   * @return
   */
  private def analyzeAndValidateProducerState(records: MemoryRecords, isFromClient: Boolean):
  (mutable.Map[Long, ProducerAppendInfo], List[CompletedTxn], Option[BatchMetadata]) = {
    val updatedProducers = mutable.Map.empty[Long, ProducerAppendInfo]
    val completedTxns = ListBuffer.empty[CompletedTxn]
    for (batch <- records.batches.asScala if batch.hasProducerId) {
      //获取此生产者最后写入的信息
      val maybeLastEntry = producerStateManager.lastEntry(batch.producerId)

      // 如果请求来源于client, 那么如果为重复请求直接返回之前追加的batch元信息
      if (isFromClient) {
        maybeLastEntry.flatMap(_.findDuplicateBatch(batch)).foreach { duplicate =>
          return (updatedProducers, completedTxns.toList, Some(duplicate))
        }
      }

      //更新生产者追加信息, 并且返回是否为事务结束
      val maybeCompletedTxn = updateProducers(batch, updatedProducers, isFromClient = isFromClient)
      //更新事务信息
      maybeCompletedTxn.foreach(completedTxns += _)
    }
    (updatedProducers, completedTxns.toList, None)
  }

  /**
   * Validate the following:
   * <ol>
   * <li> each message matches its CRC
   * <li> each message size is valid
   * <li> that the sequence numbers of the incoming record batches are consistent with the existing state and with each other.
   * </ol>
   *
   * Also compute the following quantities:
   * <ol>
   * <li> First offset in the message set
   * <li> Last offset in the message set
   * <li> Number of messages
   * <li> Number of valid bytes
   * <li> Whether the offsets are monotonically increasing
   * <li> Whether any compression codec is used (if many are used, then the last one is given)
   * </ol>
   *
   * 验证如下信息:
   * 1) 每个消息与其CRC校验码匹配;
   * 2) 每个消息的大小合法;
   * 3) 记录中的序列号与现有的状态保持一致
   *
   * 并且计算如下值:
   * 1) 第一条消息的位移;
   * 2) 最后一条消息的位移;
   * 3) 消息的个数;
   * 4) 合法的字节数大小;
   * 5) 位移是否单调递增;
   * 6) 是否使用了压缩编码
   */
  private def analyzeAndValidateRecords(records: MemoryRecords, isFromClient: Boolean): LogAppendInfo = {
    var shallowMessageCount = 0
    var validBytesCount = 0
    var firstOffset: Option[Long] = None
    var lastOffset = -1L
    var sourceCodec: CompressionCodec = NoCompressionCodec
    var monotonic = true
    var maxTimestamp = RecordBatch.NO_TIMESTAMP
    var offsetOfMaxTimestamp = -1L
    var readFirstMessage = false
    var lastOffsetOfFirstBatch = -1L

    for (batch <- records.batches.asScala) {
      // we only validate V2 and higher to avoid potential compatibility issues with older clients
      if (batch.magic >= RecordBatch.MAGIC_VALUE_V2 && isFromClient && batch.baseOffset != 0)
        throw new InvalidRecordException(s"The baseOffset of the record batch in the append to $topicPartition should " +
          s"be 0, but it is ${batch.baseOffset}")

      // update the first offset if on the first message. For magic versions older than 2, we use the last offset
      // to avoid the need to decompress the data (the last offset can be obtained directly from the wrapper message).
      // For magic version 2, we can get the first offset directly from the batch header.
      // When appending to the leader, we will update LogAppendInfo.baseOffset with the correct value. In the follower
      // case, validation will be more lenient.
      // Also indicate whether we have the accurate first offset or not
      // 如果为第一条消息, 那么更新记录的第一条位移值.
      //  1)对于版本老于2的消息, 直接使用消息包装后中携带的lastOffset;
      //  2)对于版本为2的消息, 可以直接从batch的头部读取;
      // 当追加消息到leader时, leader会更新消息的baseOffset; 对于follower来说, 则不需要做什么校验.
      if (!readFirstMessage) {
        if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
          firstOffset = Some(batch.baseOffset)
        lastOffsetOfFirstBatch = batch.lastOffset
        readFirstMessage = true
      }

      // check that offsets are monotonically increasing
      // 检查消息是否为单调递增
      if (lastOffset >= batch.lastOffset)
        monotonic = false

      // update the last offset seen
      // 更新last offset
      lastOffset = batch.lastOffset

      // Check if the message sizes are valid.
      // 检查消息大小是否合法
      val batchSize = batch.sizeInBytes
      if (batchSize > config.maxMessageSize) {
        //记录拒绝的消息大小到相应指标中
        brokerTopicStats.topicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
        brokerTopicStats.allTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
        throw new RecordTooLargeException(s"The record batch size in the append to $topicPartition is $batchSize bytes " +
          s"which exceeds the maximum configured value of ${config.maxMessageSize}.")
      }

      // check the validity of the message by checking CRC
      // 检查CRC校验码
      batch.ensureValid()

      //记录最大的时间戳
      if (batch.maxTimestamp > maxTimestamp) {
        maxTimestamp = batch.maxTimestamp
        offsetOfMaxTimestamp = lastOffset
      }

      shallowMessageCount += 1
      validBytesCount += batchSize

      //获取压缩编码
      val messageCodec = CompressionCodec.getCompressionCodec(batch.compressionType.id)
      if (messageCodec != NoCompressionCodec)
        sourceCodec = messageCodec
    }

    // Apply broker-side compression if any
    // 获取压缩编解码, 即可以保持生产者所设置的格式, 也可以使用broker侧的压缩
    val targetCodec = BrokerCompressionCodec.getTargetCompressionCodec(config.compressionType, sourceCodec)

    // 返回追加的消息信息
    LogAppendInfo(firstOffset, lastOffset, maxTimestamp, offsetOfMaxTimestamp, RecordBatch.NO_TIMESTAMP, logStartOffset,
      RecordConversionStats.EMPTY, sourceCodec, targetCodec, shallowMessageCount, validBytesCount, monotonic, lastOffsetOfFirstBatch)
  }

  /**
   * 更新生产者的追加信息
   * @param batch
   * @param producers
   * @param isFromClient
   * @return
   */
  private def updateProducers(batch: RecordBatch,
                              producers: mutable.Map[Long, ProducerAppendInfo],
                              isFromClient: Boolean): Option[CompletedTxn] = {
    val producerId = batch.producerId
    //如果此生产者没有追加过消息则新生成一个ProducerAppendInfo, 否则返回之前的ProducerAppendInfo
    val appendInfo = producers.getOrElseUpdate(producerId, producerStateManager.prepareUpdate(producerId, isFromClient))
    //更新appendInfo
    appendInfo.append(batch)
  }

  /**
   * Trim any invalid bytes from the end of this message set (if there are any)
   *
   * @param records The records to trim
   * @param info The general information of the message set
   * @return A trimmed message set. This may be the same as what was passed in or it may not.
   *
   * 去除消息尾端(trim)的冗余字节或者不完整的消息
   * 参数 records: 消息记录
   * 参数 info: 消息集的整体信息
   * 返回 trim之后的消息集
   */
  private def trimInvalidBytes(records: MemoryRecords, info: LogAppendInfo): MemoryRecords = {
    val validBytes = info.validBytes
    if (validBytes < 0)
      throw new CorruptRecordException(s"Cannot append record batch with illegal length $validBytes to " +
        s"log for $topicPartition. A possible cause is a corrupted produce request.")
    if (validBytes == records.sizeInBytes) {
      records
    } else {
      // trim invalid bytes
      // 去除尾端冗余字节
      val validByteBuffer = records.buffer.duplicate()
      validByteBuffer.limit(validBytes)
      MemoryRecords.readableRecords(validByteBuffer)
    }
  }

  /**
   * 以READ_UNCOMMITTED级别读取消息
   *
   * @param startOffset
   * @param maxLength
   * @param maxOffset
   * @param minOneMessage
   * @return
   */
  private[log] def readUncommitted(startOffset: Long, maxLength: Int, maxOffset: Option[Long] = None,
                                   minOneMessage: Boolean = false): FetchDataInfo = {
    read(startOffset, maxLength, maxOffset, minOneMessage, isolationLevel = IsolationLevel.READ_UNCOMMITTED)
  }

  /**
   * Read messages from the log.
   *
   * @param startOffset The offset to begin reading at
   * @param maxLength The maximum number of bytes to read
   * @param maxOffset The offset to read up to, exclusive. (i.e. this offset NOT included in the resulting message set)
   * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxLength` (if one exists)
   * @param isolationLevel The isolation level of the fetcher. The READ_UNCOMMITTED isolation level has the traditional
   *                       read semantics (e.g. consumers are limited to fetching up to the high watermark). In
   *                       READ_COMMITTED, consumers are limited to fetching up to the last stable offset. Additionally,
   *                       in READ_COMMITTED, the transaction index is consulted after fetching to collect the list
   *                       of aborted transactions in the fetch range which the consumer uses to filter the fetched
   *                       records before they are returned to the user. Note that fetches from followers always use
   *                       READ_UNCOMMITTED.
   * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset or before the log start offset
   * @return The fetch data information including fetch starting offset metadata and messages read.
   *
   * 从日志中读取消息
   * 参数 startOffset: 读取的起始位移
   * 参数 maxLength: 读取的最大字节数
   * 参数 maxOffset: 最大的读取位移(不包括此位移在内)
   * 参数 minOneMessage: 如果设置为true, 那么即使位移超出maxLength那么也会返回一条消息
   * 参数 isolationLevel: 拉取者设置的隔离级别. READ_UNCOMMITTED为传统的读取语义(例如消费者限制只能拉取高水位线以下的消息).
   *                     在READ_COMMITTED中, 消费者只能拉取最新stable位移以下的消息; 另外在READ_COMMITTED级别中, 会在拉取范围中
   *                     查询事务索引以收集事务终止的消息数据. follower的拉取总是READ_UNCOMMITTED级别的.
   * 抛出OffsetOutOfRangeException异常: 如果startOffset比logEndOffset更大, 或者比logStartOffset更老
   * 返回从startOffset开始的消息数据(包括消息元数据)
   */
  def read(startOffset: Long, maxLength: Int, maxOffset: Option[Long] = None, minOneMessage: Boolean = false,
           isolationLevel: IsolationLevel): FetchDataInfo = {
    maybeHandleIOException(s"Exception while reading from $topicPartition in dir ${dir.getParent}") {
      trace(s"Reading $maxLength bytes from offset $startOffset of length $size bytes")

      // Because we don't use lock for reading, the synchronization is a little bit tricky.
      // We create the local variables to avoid race conditions with updates to the log.
      // 因为读取不会加锁, 因此为了不与日志更新产生竞争, 这里使用了nextOffsetMetadata来避免竞态
      val currentNextOffsetMetadata = nextOffsetMetadata
      val next = currentNextOffsetMetadata.messageOffset

      //如果读取的起始位移等于下一条写入的位移, 那么直接返回
      if (startOffset == next) {
        val abortedTransactions =
          if (isolationLevel == IsolationLevel.READ_COMMITTED) Some(List.empty[AbortedTransaction])
          else None
        return FetchDataInfo(currentNextOffsetMetadata, MemoryRecords.EMPTY, firstEntryIncomplete = false,
          abortedTransactions = abortedTransactions)
      }

      //查找starOffset所在的日志段
      var segmentEntry = segments.floorEntry(startOffset)

      // return error on attempt to read beyond the log end offset or read below log start offset
      // 如果读取的位移比下一条写入消息位移更大, 或者比最老的消息位移老, 那么抛出OffsetOutOfRangeException异常
      if (startOffset > next || segmentEntry == null || startOffset < logStartOffset)
        throw new OffsetOutOfRangeException(s"Received request for offset $startOffset for partition $topicPartition, " +
          s"but we only have log segments in the range $logStartOffset to $next.")

      // 从比目标起始位移更小的日志段开始, 查找所需要的消息
      while (segmentEntry != null) {
        val segment = segmentEntry.getValue

        // If the fetch occurs on the active segment, there might be a race condition where two fetch requests occur after
        // the message is appended but before the nextOffsetMetadata is updated. In that case the second fetch may
        // cause OffsetOutOfRangeException. To solve that, we cap the reading up to exposed position instead of the log
        // end of the active segment.
        val maxPosition = {
          //如果目标起始位移在当前写入的日志段中, 那么使用nextOffsetMetadata作为最大查找位移; 否则查找整个日志段.
          if (segmentEntry == segments.lastEntry) {
            val exposedPos = nextOffsetMetadata.relativePositionInSegment.toLong
            // Check the segment again in case a new segment has just rolled out.
            if (segmentEntry != segments.lastEntry)
            // New log segment has rolled out, we can read up to the file end.
              segment.size
            else
              exposedPos
          } else {
            segment.size
          }
        }
        //读取日志段, 获取希望拉取的消息信息
        val fetchInfo = segment.read(startOffset, maxOffset, maxLength, maxPosition, minOneMessage)

        if (fetchInfo == null) {
          //如果当前日志段查询不到, 那么查询下一个日志段
          segmentEntry = segments.higherEntry(segmentEntry.getKey)
        } else {
          //如果隔离级别为READ_UNCOMMITTED那么直接返回, 否则查询事务索引增加与拉取范围有重叠的已终止(aborted)事务信息
          return isolationLevel match {
            case IsolationLevel.READ_UNCOMMITTED => fetchInfo
            case IsolationLevel.READ_COMMITTED => addAbortedTransactions(startOffset, segmentEntry, fetchInfo)
          }
        }
      }

      // okay we are beyond the end of the last segment with no data fetched although the start offset is in range,
      // this can happen when all messages with offset larger than start offsets have been deleted.
      // In this case, we will return the empty set with log end offset metadata
      // 到达这个分支意味着, 虽然拉取的起始位移是在日志段的位移范围内, 但仍然查找所有的日志段都没有数据可读.
      // 当所有比起始位移大的消息被删除时, 就会发生这样的情况. 这里直接返回空数据.
      FetchDataInfo(nextOffsetMetadata, MemoryRecords.EMPTY)
    }
  }

  /**
   * 获取从startOffset(包含)到upperBoundOffset(不包含)之间已回滚的事务
   * @param startOffset
   * @param upperBoundOffset
   * @return
   */
  private[log] def collectAbortedTransactions(startOffset: Long, upperBoundOffset: Long): List[AbortedTxn] = {
    val segmentEntry = segments.floorEntry(startOffset)
    val allAbortedTxns = ListBuffer.empty[AbortedTxn]
    def accumulator(abortedTxns: List[AbortedTxn]): Unit = allAbortedTxns ++= abortedTxns
    collectAbortedTransactions(logStartOffset, upperBoundOffset, segmentEntry, accumulator)
    allAbortedTxns.toList
  }

  private def addAbortedTransactions(startOffset: Long, segmentEntry: JEntry[JLong, LogSegment],
                                     fetchInfo: FetchDataInfo): FetchDataInfo = {
    //获取一个大于拉取范围的位移上限(该上限不一定是最小的位移上限)
    val fetchSize = fetchInfo.records.sizeInBytes
    val startOffsetPosition = OffsetPosition(fetchInfo.fetchOffsetMetadata.messageOffset,
      fetchInfo.fetchOffsetMetadata.relativePositionInSegment)
    val upperBoundOffset = segmentEntry.getValue.fetchUpperBoundOffset(startOffsetPosition, fetchSize).getOrElse {
      val nextSegmentEntry = segments.higherEntry(segmentEntry.getKey)
      if (nextSegmentEntry != null)
        nextSegmentEntry.getValue.baseOffset
      else
        logEndOffset
    }

    //获取与拉取范围[startOffset,upperBoundOffset) 有重叠的已回滚事务
    val abortedTransactions = ListBuffer.empty[AbortedTransaction]
    def accumulator(abortedTxns: List[AbortedTxn]): Unit = abortedTransactions ++= abortedTxns.map(_.asAbortedTransaction)
    collectAbortedTransactions(startOffset, upperBoundOffset, segmentEntry, accumulator)

    //返回带有已回滚事务的拉取信息
    FetchDataInfo(fetchOffsetMetadata = fetchInfo.fetchOffsetMetadata,
      records = fetchInfo.records,
      firstEntryIncomplete = fetchInfo.firstEntryIncomplete,
      abortedTransactions = Some(abortedTransactions.toList))
  }

  /**
   * 查询拉取范围内的已回滚事务
   *
   * @param startOffset: 拉取范围起始端(包含)
   * @param upperBoundOffset: 拉取范围结束端(不包含)
   * @param startingSegmentEntry: 开始查询的日志段
   * @param accumulator: 已回滚事务的收集器
   */
  private def collectAbortedTransactions(startOffset: Long, upperBoundOffset: Long,
                                         startingSegmentEntry: JEntry[JLong, LogSegment],
                                         accumulator: List[AbortedTxn] => Unit): Unit = {
    var segmentEntry = startingSegmentEntry
    while (segmentEntry != null) {
      val searchResult = segmentEntry.getValue.collectAbortedTxns(startOffset, upperBoundOffset)
      accumulator(searchResult.abortedTransactions)
      if (searchResult.isComplete)
        return
      segmentEntry = segments.higherEntry(segmentEntry.getKey)
    }
  }

  /**
   * Get an offset based on the given timestamp
   * The offset returned is the offset of the first message whose timestamp is greater than or equals to the
   * given timestamp.
   *
   * If no such message is found, the log end offset is returned.
   *
   * `NOTE:` OffsetRequest V0 does not use this method, the behavior of OffsetRequest V0 remains the same as before
   * , i.e. it only gives back the timestamp based on the last modification time of the log segments.
   *
   * @param targetTimestamp The given timestamp for offset fetching.
   * @return The offset of the first message whose timestamp is greater than or equals to the given timestamp.
   *         None if no such message is found.
   *
   * 获取时间戳大于等于指定时间戳的第一条消息位移. 如果没有找到这样的消息, 那么返回logEndOffset.
   * 注意: V0版本的OffsetRequest不使用此方法, 它的行为和以前保持一致, 那就是基于日志段最后的修改时间返回时间戳.
   *
   * 参数 targetTimestamp: 希望拉取的消息时间戳
   * 返回: 时间戳大于等于指定时间戳的第一条消息位移
   */
  def fetchOffsetsByTimestamp(targetTimestamp: Long): Option[TimestampOffset] = {
    maybeHandleIOException(s"Error while fetching offset by timestamp for $topicPartition in dir ${dir.getParent}") {
      debug(s"Searching offset for timestamp $targetTimestamp")

      //KAFKA_0_10_0_IV0版本小的消息不支持基于时间戳搜索
      if (config.messageFormatVersion < KAFKA_0_10_0_IV0 &&
        targetTimestamp != ListOffsetRequest.EARLIEST_TIMESTAMP &&
        targetTimestamp != ListOffsetRequest.LATEST_TIMESTAMP)
        throw new UnsupportedForMessageFormatException(s"Cannot search offsets based on timestamp because message format version " +
          s"for partition $topicPartition is ${config.messageFormatVersion} which is earlier than the minimum " +
          s"required version $KAFKA_0_10_0_IV0")

      // Cache to avoid race conditions. `toBuffer` is faster than most alternatives and provides
      // constant time access while being safe to use with concurrent collections unlike `toArray`.
      // 复制避免竞争. `toBuffer`比其它方法都高效, 并且提供常量的访问时间, 同时线程安全(不像`toArray`)
      val segmentsCopy = logSegments.toBuffer

      // For the earliest and latest, we do not need to return the timestamp.
      // 如果为获取最早和最新的位移, 不需要返回时间戳
      if (targetTimestamp == ListOffsetRequest.EARLIEST_TIMESTAMP)
        return Some(TimestampOffset(RecordBatch.NO_TIMESTAMP, logStartOffset))
      else if (targetTimestamp == ListOffsetRequest.LATEST_TIMESTAMP)
        return Some(TimestampOffset(RecordBatch.NO_TIMESTAMP, logEndOffset))

      val targetSeg = {
        // Get all the segments whose largest timestamp is smaller than target timestamp
        // 找到所有最大时间戳比目标时间戳小的日志段
        val earlierSegs = segmentsCopy.takeWhile(_.largestTimestamp < targetTimestamp)

        // We need to search the first segment whose largest timestamp is greater than the target timestamp if there is one.
        // 获取第一个最大时间戳比目标时间戳大的日志段, 如果不存在则为None
        if (earlierSegs.length < segmentsCopy.length)
          Some(segmentsCopy(earlierSegs.length))
        else
          None
      }

      //如果存在, 调用该日志段的findOffsetByTimestamp方法, 查询时间戳大于等于指定时间戳的第一条消息位移
      targetSeg.flatMap(_.findOffsetByTimestamp(targetTimestamp, logStartOffset))
    }
  }

  /**
   * Given a message offset, find its corresponding offset metadata in the log.
   * If the message offset is out of range, return None to the caller.
   */
  def convertToOffsetMetadata(offset: Long): Option[LogOffsetMetadata] = {
    try {
      val fetchDataInfo = readUncommitted(offset, 1)
      Some(fetchDataInfo.fetchOffsetMetadata)
    } catch {
      case _: OffsetOutOfRangeException => None
    }
  }

  /**
   * Delete any log segments matching the given predicate function,
   * starting with the oldest segment and moving forward until a segment doesn't match.
   *
   * @param predicate A function that takes in a candidate log segment and the next higher segment
   *                  (if there is one) and returns true iff it is deletable
   * @return The number of segments deleted
   */
  private def deleteOldSegments(predicate: (LogSegment, Option[LogSegment]) => Boolean, reason: String): Int = {
    lock synchronized {
      val deletable = deletableSegments(predicate)
      if (deletable.nonEmpty)
        info(s"Found deletable segments with base offsets [${deletable.map(_.baseOffset).mkString(",")}] due to $reason")
      deleteSegments(deletable)
    }
  }

  private def deleteSegments(deletable: Iterable[LogSegment]): Int = {
    maybeHandleIOException(s"Error while deleting segments for $topicPartition in dir ${dir.getParent}") {
      val numToDelete = deletable.size
      if (numToDelete > 0) {
        // we must always have at least one segment, so if we are going to delete all the segments, create a new one first
        if (segments.size == numToDelete)
          roll()
        lock synchronized {
          checkIfMemoryMappedBufferClosed()
          // remove the segments for lookups
          deletable.foreach(deleteSegment)
          maybeIncrementLogStartOffset(segments.firstEntry.getValue.baseOffset)
        }
      }
      numToDelete
    }
  }

  /**
   * Find segments starting from the oldest until the user-supplied predicate is false or the segment
   * containing the current high watermark is reached. We do not delete segments with offsets at or beyond
   * the high watermark to ensure that the log start offset can never exceed it. If the high watermark
   * has not yet been initialized, no segments are eligible for deletion.
   *
   * A final segment that is empty will never be returned (since we would just end up re-creating it).
   *
   * @param predicate A function that takes in a candidate log segment and the next higher segment
   *                  (if there is one) and returns true iff it is deletable
   * @return the segments ready to be deleted
   */
  private def deletableSegments(predicate: (LogSegment, Option[LogSegment]) => Boolean): Iterable[LogSegment] = {
    if (segments.isEmpty || replicaHighWatermark.isEmpty) {
      Seq.empty
    } else {
      val highWatermark = replicaHighWatermark.get
      val deletable = ArrayBuffer.empty[LogSegment]
      var segmentEntry = segments.firstEntry
      while (segmentEntry != null) {
        val segment = segmentEntry.getValue
        val nextSegmentEntry = segments.higherEntry(segmentEntry.getKey)
        val (nextSegment, upperBoundOffset, isLastSegmentAndEmpty) = if (nextSegmentEntry != null)
          (nextSegmentEntry.getValue, nextSegmentEntry.getValue.baseOffset, false)
        else
          (null, logEndOffset, segment.size == 0)

        if (highWatermark >= upperBoundOffset && predicate(segment, Option(nextSegment)) && !isLastSegmentAndEmpty) {
          deletable += segment
          segmentEntry = nextSegmentEntry
        } else {
          segmentEntry = null
        }
      }
      deletable
    }
  }

  /**
   * Delete any log segments that have either expired due to time based retention
   * or because the log size is > retentionSize
   */
  def deleteOldSegments(): Int = {
    if (!config.delete) return 0
    deleteRetentionMsBreachedSegments() + deleteRetentionSizeBreachedSegments() + deleteLogStartOffsetBreachedSegments()
  }

  private def deleteRetentionMsBreachedSegments(): Int = {
    if (config.retentionMs < 0) return 0
    val startMs = time.milliseconds
    deleteOldSegments((segment, _) => startMs - segment.largestTimestamp > config.retentionMs,
      reason = s"retention time ${config.retentionMs}ms breach")
  }

  private def deleteRetentionSizeBreachedSegments(): Int = {
    if (config.retentionSize < 0 || size < config.retentionSize) return 0
    var diff = size - config.retentionSize
    def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]) = {
      if (diff - segment.size >= 0) {
        diff -= segment.size
        true
      } else {
        false
      }
    }

    deleteOldSegments(shouldDelete, reason = s"retention size in bytes ${config.retentionSize} breach")
  }

  private def deleteLogStartOffsetBreachedSegments(): Int = {
    def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]) =
      nextSegmentOpt.exists(_.baseOffset <= logStartOffset)

    deleteOldSegments(shouldDelete, reason = s"log start offset $logStartOffset breach")
  }

  def isFuture: Boolean = dir.getName.endsWith(Log.FutureDirSuffix)

  /**
   * The size of the log in bytes
   */
  def size: Long = Log.sizeInBytes(logSegments)

  /**
   * The offset metadata of the next message that will be appended to the log
   */
  def logEndOffsetMetadata: LogOffsetMetadata = nextOffsetMetadata

  /**
   * The offset of the next message that will be appended to the log
   * 下一条消息的位移
   */
  def logEndOffset: Long = nextOffsetMetadata.messageOffset

  /**
   * Roll the log over to a new empty log segment if necessary.
   *
   * @param messagesSize The messages set size in bytes.
   * @param appendInfo log append information
   * logSegment will be rolled if one of the following conditions met
   * <ol>
   * <li> The logSegment is full
   * <li> The maxTime has elapsed since the timestamp of first message in the segment (or since the create time if
   * the first message does not have a timestamp)
   * <li> The index is full
   * </ol>
   * @return The currently active segment after (perhaps) rolling to a new segment
   *
   * 判断是否需要滚动日志生成新的日志段, 如果需要则返回新的日志段, 否则返回老的日志段.
   * 参数 messagesSize: 消息集的字节数
   * 参数 appendInfo: 追加的相关信息
   * 发生如下情况时, 会滚动日志段:
   *   1) 日志段满了
   *   2) 追加消息的时间与日志段第一条消息的时间(如果第一条消息没有时间戳, 则为创建时间)的差值超过了maxTime阈值
   *   3) 日志索引满了
   */
  private def maybeRoll(messagesSize: Int, appendInfo: LogAppendInfo): LogSegment = {
    val segment = activeSegment
    val now = time.milliseconds

    val maxTimestampInMessages = appendInfo.maxTimestamp
    val maxOffsetInMessages = appendInfo.lastOffset

    if (segment.shouldRoll(messagesSize, maxTimestampInMessages, maxOffsetInMessages, now)) {
      //如果需要滚动日志段, 那么调用roll方法进行滚动

      debug(s"Rolling new log segment (log_size = ${segment.size}/${config.segmentSize}}, " +
        s"offset_index_size = ${segment.offsetIndex.entries}/${segment.offsetIndex.maxEntries}, " +
        s"time_index_size = ${segment.timeIndex.entries}/${segment.timeIndex.maxEntries}, " +
        s"inactive_time_ms = ${segment.timeWaitedForRoll(now, maxTimestampInMessages)}/${config.segmentMs - segment.rollJitterMs}).")

      appendInfo.firstOffset match {
        case Some(firstOffset) => roll(firstOffset)

        //(maxOffsetInMessages - Integer.MAX_VALUE)是消息集中第一条消息位移的启发标识. 由于消息集中位移差值不会超过Integer.MAX_VALUE,
        //因此这个值可以保证小于等于消息集中的第一条消息位移. 获取消息集的真正第一条消息位移需要解压缩, 这个操作在follower上是尽量避免
        //的行为. 之前的行为是使用老日志段的结束位移来设置新日志段的基准位移(即baseOffset = logEndOffset), 但老的做法在连续两条消息
        //的位移差值为Integer.MAX_VALUE.toLong + 2(或者更大)时会有问题, 这会导致滚动生成的新日志段的基准位移太低而不能写入当前的消息.
        //而这种情景是存在的, 比如一个副本机器试图恢复一个高度compact的主题.
        //这些情况只会发生在V2版本以前的消息, 因为老版本不会在header中存储第一条消息的位移
        case None => roll(maxOffsetInMessages - Integer.MAX_VALUE)
      }
    } else {
      //如果不需要滚动, 则返回当前的日志段
      segment
    }
  }

  /**
   * Roll the log over to a new active segment starting with the current logEndOffset.
   * This will trim the index to the exact size of the number of entries it currently contains.
   *
   * @return The newly rolled segment
   *
   * 滚动日志, 生成一个新的日志段, 新日志段的基准位移取Max(当前日志的logEndOffset, 参数的expectedNextOffset).
   * 同时此方法会缩减位移文件, 删除冗余的空间.
   */
  def roll(expectedNextOffset: Long = 0): LogSegment = {
    maybeHandleIOException(s"Error while rolling log segment for $topicPartition in dir ${dir.getParent}") {
      //获取开始时间
      val start = time.hiResClockMs()

      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        //新日志段的位移
        val newOffset = math.max(expectedNextOffset, logEndOffset)
        //生成日志文件
        val logFile = Log.logFile(dir, newOffset)
        //生成索引文件
        val offsetIdxFile = offsetIndexFile(dir, newOffset)
        //生成时间索引文件
        val timeIdxFile = timeIndexFile(dir, newOffset)
        //生成事务索引文件
        val txnIdxFile = transactionIndexFile(dir, newOffset)

        for (file <- List(logFile, offsetIdxFile, timeIdxFile, txnIdxFile) if file.exists) {
          warn(s"Newly rolled segment file ${file.getAbsolutePath} already exists; deleting it first")
          Files.delete(file.toPath)
        }

        //对正在使用的日志段调用onBecomeInactiveSegment方法
        Option(segments.lastEntry).foreach(_.getValue.onBecomeInactiveSegment())

        // take a snapshot of the producer state to facilitate recovery. It is useful to have the snapshot
        // offset align with the new segment offset since this ensures we can recover the segment by beginning
        // with the corresponding snapshot file and scanning the segment data. Because the segment base offset
        // may actually be ahead of the current producer state end offset (which corresponds to the log end offset),
        // we manually override the state offset here prior to taking the snapshot.
        // 对生产者状态取快照可以加速故障恢复. 将快照的位移设置成新日志段的基准位移是很有帮助的, 因为这样在恢复日志段时, 可以
        // 根据日志段的位移来查找相应的快照. 而由于日志段基准位移可能会比当前生产者结束位移(即logEndOffset)大, 因此这里先更新
        // 位移再生成快照
        producerStateManager.updateMapEndOffset(newOffset)
        producerStateManager.takeSnapshot()

        // 创建新的日志段
        val segment = LogSegment.open(dir,
          baseOffset = newOffset,
          config,
          time = time,
          fileAlreadyExists = false,
          initFileSize = initFileSize,
          preallocate = config.preallocate)
        //增加此新的日志段
        val prev = addSegment(segment)
        if (prev != null)
          throw new KafkaException(s"Trying to roll a new log segment for topic partition $topicPartition with " +
            s"start offset $newOffset while it already exists.")
        // We need to update the segment base offset and append position data of the metadata when log rolls.
        // The next offset should not change.
        // 更新下一条日志的元数据, nextOffset一样保持不变, 更新日志段信息
        updateLogEndOffset(nextOffsetMetadata.messageOffset)

        // schedule an asynchronous flush of the old segment
        // 老日志段异步刷新
        scheduler.schedule("flush-log", () => flush(newOffset), delay = 0L)

        info(s"Rolled new log segment at offset $newOffset in ${time.hiResClockMs() - start} ms.")

        segment
      }
    }
  }

  /**
   * The number of messages appended to the log since the last flush
   */
  def unflushedMessages: Long = this.logEndOffset - this.recoveryPoint

  /**
   * Flush all log segments
   * 将所有的日志段刷盘
   */
  def flush(): Unit = flush(this.logEndOffset)

  /**
   * Flush log segments for all offsets up to offset-1
   *
   * @param offset The offset to flush up to (non-inclusive); the new recovery point
   *
   * 将位移小于offset的日志段刷盘
   */
  def flush(offset: Long) : Unit = {
    maybeHandleIOException(s"Error while flushing log for $topicPartition in dir ${dir.getParent} with offset $offset") {
      //如果offset比上一次的刷盘点小, 那么直接返回
      if (offset <= this.recoveryPoint)
        return
      debug(s"Flushing log up to offset $offset, last flushed: $lastFlushTime,  current time: ${time.milliseconds()}, " +
        s"unflushed: $unflushedMessages")
      //将位移小于offset的日志段刷盘
      for (segment <- logSegments(this.recoveryPoint, offset))
        segment.flush()

      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        //记录新的刷盘点
        if (offset > this.recoveryPoint) {
          this.recoveryPoint = offset
          lastFlushedTime.set(time.milliseconds)
        }
      }
    }
  }

  /**
   * Cleanup old producer snapshots after the recovery point is checkpointed. It is useful to retain
   * the snapshots from the recent segments in case we need to truncate and rebuild the producer state.
   * Otherwise, we would always need to rebuild from the earliest segment.
   *
   * More specifically:
   *
   * 1. We always retain the producer snapshot from the last two segments. This solves the common case
   * of truncating to an offset within the active segment, and the rarer case of truncating to the previous segment.
   *
   * 2. We only delete snapshots for offsets less than the recovery point. The recovery point is checkpointed
   * periodically and it can be behind after a hard shutdown. Since recovery starts from the recovery point, the logic
   * of rebuilding the producer snapshots in one pass and without loading older segments is simpler if we always
   * have a producer snapshot for all segments being recovered.
   *
   * Return the minimum snapshots offset that was retained.
   *
   * 在恢复点前的老的生产者快照. 保留最近日志段的生产者快照是非常有用的, 这样我们在日志截断时可以使用来重建生产者状态, 否则
   * 需要从最早的日志段开始来重建.
   *
   * 准确来说:
   * 1. 这里保留最近两个日志段的生产者快照. 这样就包括了最常见的当前日志段阶段的场景, 以及非常少见的截断至前一个日志段的场景.
   * 2. 这里只删除小于恢复点的快照. 恢复点会定期被持久化, 可以用来在意外宕机后做故障恢复. 由于故障恢复时从恢复点开始, 因此
   *    如果对于所有的恢复日志段来说只保留一个生产者快照, 那么重建生产者状态会更简单, 而且不需要加载老的日志段.
   */
  def deleteSnapshotsAfterRecoveryPointCheckpoint(): Long = {
    //获取需要保留的最小快照位移
    val minOffsetToRetain = minSnapshotsOffsetToRetain

    //删除此之前的生产者快照
    producerStateManager.deleteSnapshotsBefore(minOffsetToRetain)

    //返回保留的最小位移
    minOffsetToRetain
  }

  // Visible for testing, see `deleteSnapshotsAfterRecoveryPointCheckpoint()` for details
  // 对测试可见, 此方法使用详见deleteSnapshotsAfterRecoveryPointCheckpoint内部实现
  // 获取需要保留的最小快照位移
  private[log] def minSnapshotsOffsetToRetain: Long = {
    lock synchronized {
      //获取前一个日志段的基准位移, 如果不存在前一个日志段则使用当前日志段的基准位移
      val twoSegmentsMinOffset = lowerSegment(activeSegment.baseOffset).getOrElse(activeSegment).baseOffset

      // Prefer segment base offset
      // 获取基准位移小于恢复点的最大日志段基准位移, 如果不存在则使用恢复点
      val recoveryPointOffset = lowerSegment(recoveryPoint).map(_.baseOffset).getOrElse(recoveryPoint)

      //返回recoveryPointOffset和twoSegmentsMinOffset的最小值
      math.min(recoveryPointOffset, twoSegmentsMinOffset)
    }
  }

  /**
   * 获取基准位移比参数offset小的最大日志段
   * @param offset
   * @return
   */
  private def lowerSegment(offset: Long): Option[LogSegment] =
    Option(segments.lowerEntry(offset)).map(_.getValue)

  /**
   * Completely delete this log directory and all contents from the file system with no delay
   * 删除此日志目录和目录中的所有内容
   */
  private[log] def delete() {
    maybeHandleIOException(s"Error while deleting log for $topicPartition in dir ${dir.getParent}") {
      lock synchronized {
        //检查内存映射缓冲区是否关闭
        checkIfMemoryMappedBufferClosed()

        //删除日志指标
        removeLogMetrics()

        //删除所有日志段数据
        logSegments.foreach(_.deleteIfExists())

        //清除内存中记录的日志段数据
        segments.clear()

        //清除epoch与其初始位移的映射缓存
        _leaderEpochCache.clear()

        //删除目录
        Utils.delete(dir)

        // File handlers will be closed if this log is deleted
        isMemoryMappedBufferClosed = true
      }
    }
  }

  // visible for testing
  // 生成生产者快照
  private[log] def takeProducerSnapshot(): Unit = lock synchronized {
    checkIfMemoryMappedBufferClosed()
    producerStateManager.takeSnapshot()
  }

  // visible for testing
  // 获取生产者最新的快照位移
  private[log] def latestProducerSnapshotOffset: Option[Long] = lock synchronized {
    producerStateManager.latestSnapshotOffset
  }

  // visible for testing
  // 获取生产者最老的快照位移
  private[log] def oldestProducerSnapshotOffset: Option[Long] = lock synchronized {
    producerStateManager.oldestSnapshotOffset
  }

  // visible for testing
  // 获取生产者的最新位移
  private[log] def latestProducerStateEndOffset: Long = lock synchronized {
    producerStateManager.mapEndOffset
  }

  /**
   * Truncate this log so that it ends with the greatest offset < targetOffset.
   *
   * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete.
   * @return True iff targetOffset < logEndOffset
   *
   * 将此日志截断, 使其最大位移小于目标位移
   *
   * 参数 targetOffset: 目标位移, 日志截断后的所有位移上限
   * 返回 true当且仅当目标位移小于logEndOffset(日志的下一条消息位移)
   */
  private[log] def truncateTo(targetOffset: Long): Boolean = {
    maybeHandleIOException(s"Error while truncating log to offset $targetOffset for $topicPartition in dir ${dir.getParent}") {
      if (targetOffset < 0)
        throw new IllegalArgumentException(s"Cannot truncate partition $topicPartition to a negative offset (%d).".format(targetOffset))
      if (targetOffset >= logEndOffset) {
        info(s"Truncating to $targetOffset has no effect as the largest offset in the log is ${logEndOffset - 1}")
        false
      } else {
        info(s"Truncating to offset $targetOffset")
        lock synchronized {
          checkIfMemoryMappedBufferClosed()
          if (segments.firstEntry.getValue.baseOffset > targetOffset) {
            truncateFullyAndStartAt(targetOffset)
          } else {
            val deletable = logSegments.filter(segment => segment.baseOffset > targetOffset)
            deletable.foreach(deleteSegment)
            activeSegment.truncateTo(targetOffset)
            updateLogEndOffset(targetOffset)
            this.recoveryPoint = math.min(targetOffset, this.recoveryPoint)
            this.logStartOffset = math.min(targetOffset, this.logStartOffset)
            _leaderEpochCache.clearAndFlushLatest(targetOffset)
            loadProducerState(targetOffset, reloadFromCleanShutdown = false)
          }
          true
        }
      }
    }
  }

  /**
   *  Delete all data in the log and start at the new offset
   *
   *  @param newOffset The new offset to start the log with
   *
   *  删除日志中所有的数据, 并且从新的位移重新开始
   *  参数 newOffset: 新日志的开始位移
   */
  private[log] def truncateFullyAndStartAt(newOffset: Long) {
    maybeHandleIOException(s"Error while truncating the entire log for $topicPartition in dir ${dir.getParent}") {
      debug(s"Truncate and start at offset $newOffset")
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        val segmentsToDelete = logSegments.toList
        //异步删除日志段
        segmentsToDelete.foreach(deleteSegment)

        //以新的位移生成新日志段
        addSegment(LogSegment.open(dir,
          baseOffset = newOffset,
          config = config,
          time = time,
          fileAlreadyExists = false,
          initFileSize = initFileSize,
          preallocate = config.preallocate))

        //更新日志结束位移
        updateLogEndOffset(newOffset)

        //清除leader epoch缓存信息
        _leaderEpochCache.clearAndFlush()

        //截断清理生产者状态, 重置其mapEndOffset
        producerStateManager.truncate()
        producerStateManager.updateMapEndOffset(newOffset)
        //更新第一条unstable消息位移
        updateFirstUnstableOffset()

        //更新恢复点
        this.recoveryPoint = math.min(newOffset, this.recoveryPoint)

        //更新日志最早位移
        this.logStartOffset = newOffset
      }
    }
  }

  /**
   * The time this log is last known to have been fully flushed to disk
   * 此日志上一次刷盘时间点
   */
  def lastFlushTime: Long = lastFlushedTime.get

  /**
   * The active segment that is currently taking appends
   * 当前追加消息的日志段
   */
  def activeSegment = segments.lastEntry.getValue

  /**
   * All the log segments in this log ordered from oldest to newest
   * 此日志的所有日志段, 从最老到最新
   */
  def logSegments: Iterable[LogSegment] = segments.values.asScala

  /**
   * Get all segments beginning with the segment that includes "from" and ending with the segment
   * that includes up to "to-1" or the end of the log (if to > logEndOffset)
   */
  //返回基准位移在[from, to) 之间的日志段, 注意这里包括from但不包括to
  def logSegments(from: Long, to: Long): Iterable[LogSegment] = {
    lock synchronized {
      val view = Option(segments.floorKey(from)).map { floor =>
        segments.subMap(floor, to)
      }.getOrElse(segments.headMap(to))
      view.values.asScala
    }
  }

  override def toString = "Log(" + dir + ")"

  /**
   * This method performs an asynchronous log segment delete by doing the following:
   * <ol>
   *   <li>It removes the segment from the segment map so that it will no longer be used for reads.
   *   <li>It renames the index and log files by appending .deleted to the respective file name
   *   <li>It schedules an asynchronous delete operation to occur in the future
   * </ol>
   * This allows reads to happen concurrently without synchronization and without the possibility of physically
   * deleting a file while it is being read from.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is either called before all logs are loaded
   * or the immediate caller will catch and handle IOException
   *
   * @param segment The log segment to schedule for deletion
   *
   * 此方法执行异步的日志段删除操作, 如下所示:
   *
   * 1) 从日志段map中删除日志段, 这样该日志段将不会用来做读取;
   * 2) 将索引和日志文件重命名添加.deleted后缀;
   * 3) 执行一个异步的删除操作
   *
   * 此方法允许和读取操作同时操作而不需要做同步, 也不存在物理删除操作与读取操作冲突的可能性.
   * 此方法不需要将IOException转换成KafkaStorageException, 因为它只会在所有日志加载前调用或者调用者本身会处理IOException
   *
   * 参数 segment: 需要删除的日志段
   */
  private def deleteSegment(segment: LogSegment) {
    info(s"Scheduling log segment [baseOffset ${segment.baseOffset}, size ${segment.size}] for deletion.")
    lock synchronized {
      segments.remove(segment.baseOffset)
      asyncDeleteSegment(segment)
    }
  }

  /**
   * Perform an asynchronous delete on the given file if it exists (otherwise do nothing)
   *
   * This method does not need to convert IOException (thrown from changeFileSuffixes) to KafkaStorageException because
   * it is either called before all logs are loaded or the caller will catch and handle IOException
   *
   * @throws IOException if the file can't be renamed and still exists
   *
   * 异步删除日志段文件
   */
  private def asyncDeleteSegment(segment: LogSegment) {
    segment.changeFileSuffixes("", Log.DeletedFileSuffix)
    def deleteSeg() {
      info(s"Deleting segment ${segment.baseOffset}")
      maybeHandleIOException(s"Error while deleting segments for $topicPartition in dir ${dir.getParent}") {
        segment.deleteIfExists()
      }
    }

    //执行调度任务, 底下使用java.util.concurrent.ScheduledThreadPoolExecutor实现
    scheduler.schedule("delete-file", deleteSeg _, delay = config.fileDeleteDelayMs)
  }

  /**
   * Swap one or more new segment in place and delete one or more existing segments in a crash-safe manner. The old
   * segments will be asynchronously deleted.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is either called before all logs are loaded
   * or the caller will catch and handle IOException
   *
   * The sequence of operations is:
   * <ol>
   *   <li> Cleaner creates one or more new segments with suffix .cleaned and invokes replaceSegments().
   *        If broker crashes at this point, the clean-and-swap operation is aborted and
   *        the .cleaned files are deleted on recovery in loadSegments().
   *   <li> New segments are renamed .swap. If the broker crashes before all segments were renamed to .swap, the
   *        clean-and-swap operation is aborted - .cleaned as well as .swap files are deleted on recovery in
   *        loadSegments(). We detect this situation by maintaining a specific order in which files are renamed from
   *        .cleaned to .swap. Basically, files are renamed in descending order of offsets. On recovery, all .swap files
   *        whose offset is greater than the minimum-offset .clean file are deleted.
   *   <li> If the broker crashes after all new segments were renamed to .swap, the operation is completed, the swap
   *        operation is resumed on recovery as described in the next step.
   *   <li> Old segment files are renamed to .deleted and asynchronous delete is scheduled.
   *        If the broker crashes, any .deleted files left behind are deleted on recovery in loadSegments().
   *        replaceSegments() is then invoked to complete the swap with newSegment recreated from
   *        the .swap file and oldSegments containing segments which were not renamed before the crash.
   *   <li> Swap segment(s) are renamed to replace the existing segments, completing this operation.
   *        If the broker crashes, any .deleted files which may be left behind are deleted
   *        on recovery in loadSegments().
   * </ol>
   *
   * @param newSegments The new log segment to add to the log
   * @param oldSegments The old log segments to delete from the log
   * @param isRecoveredSwapFile true if the new segment was created from a swap file during recovery after a crash
   *
   * 将新的日志段替代老的日志段, 此方法与加载过程中的loadSegments方法结合, 能够保证即便执行过程中broker退出也不会出现任何问题
   *
   * 操作步骤为:
   *   1) 首先在调用此方法前, Cleaner会创建若干个以.cleaned结尾的日志段文件, 然后调用此方法,如果此方法还没开始执行时broker发生故障,
   *      .cleaned文件将会在loadSegments()方法中删除;
   *   2) 如果在把这些.cleaned文件重命名为.swap过程中broker发生故障, .swap文件和.cleaned文件
   *      都会被删除. 检测办法为: 在重命名时以位移逆序方式进行, 这样在故障恢复时可以检测.swap文件和.cleaned文件的位移大小并删除;
   *   3) 如果所有新的日志段文件都被重命名为.swap, 此时broker发生故障, 那么故障恢复的步骤和下一个步骤相同;
   *   4) 老的日志段文件会被重命名为.deleted文件, 并触发异步的物理删除任务. 如果异步任务执行过程中broker故障, 那么会在故障恢复时在
   *      loadSegments()方法中删除;
   *   5) swap日志段会被重命名, 删除其.swap后缀, 替换过程完成
   *
   * 参数 newSegments: 新的日志段
   * 参数 oldSegments: 老的日志段
   * 参数 isRecoveredSwapFile 是否为从.swap文件中恢复, true表示从之前被中断的replaceSegments中恢复
   *
   */
  private[log] def replaceSegments(newSegments: Seq[LogSegment], oldSegments: Seq[LogSegment], isRecoveredSwapFile: Boolean = false) {
    val sortedNewSegments = newSegments.sortBy(_.baseOffset)
    val sortedOldSegments = oldSegments.sortBy(_.baseOffset)

    lock synchronized {
      checkIfMemoryMappedBufferClosed()
      // need to do this in two phases to be crash safe AND do the delete asynchronously
      // if we crash in the middle of this we complete the swap in loadSegments()
      //
      // 如果不是从.swap文件中恢复, 那么先将.cleaned文件转换为.swap文件, 然后删除内存中的segments信息,
      // 最后才将.swap文件安全命名为正式段文件
      if (!isRecoveredSwapFile)
        sortedNewSegments.reverse.foreach(_.changeFileSuffixes(Log.CleanedFileSuffix, Log.SwapFileSuffix))
      sortedNewSegments.reverse.foreach(addSegment(_))

      // delete the old files
      // 移除老的日志段, 将老的日志段文件重命名为.deleted, 并异步删除文件
      for (seg <- sortedOldSegments) {
        // remove the index entry
        if (seg.baseOffset != sortedNewSegments.head.baseOffset)
          segments.remove(seg.baseOffset)
        // delete segment
        asyncDeleteSegment(seg)
      }
      // okay we are safe now, remove the swap suffix
      // 将.swap文件重命名为正式段文件
      sortedNewSegments.foreach(_.changeFileSuffixes(Log.SwapFileSuffix, ""))
    }
  }

  /**
   * remove deleted log metrics
   * 删除日志指标
   */
  private[log] def removeLogMetrics(): Unit = {
    removeMetric("NumLogSegments", tags)
    removeMetric("LogStartOffset", tags)
    removeMetric("LogEndOffset", tags)
    removeMetric("Size", tags)
  }

  /**
   * Add the given segment to the segments in this log. If this segment replaces an existing segment, delete it.
   * @param segment The segment to add
   */
  @threadsafe
  def addSegment(segment: LogSegment): LogSegment = this.segments.put(segment.baseOffset, segment)

  private def maybeHandleIOException[T](msg: => String)(fun: => T): T = {
    try {
      fun
    } catch {
      case e: IOException =>
        logDirFailureChannel.maybeAddOfflineLogDir(dir.getParent, msg, e)
        throw new KafkaStorageException(msg, e)
    }
  }

  private[log] def retryOnOffsetOverflow[T](fn: => T): T = {
    while (true) {
      try {
        return fn
      } catch {
        case e: LogSegmentOffsetOverflowException =>
          info(s"Caught segment overflow error: ${e.getMessage}. Split segment and retry.")
          splitOverflowedSegment(e.segment)
      }
    }
    throw new IllegalStateException()
  }

  /**
   * Split a segment into one or more segments such that there is no offset overflow in any of them. The
   * resulting segments will contain the exact same messages that are present in the input segment. On successful
   * completion of this method, the input segment will be deleted and will be replaced by the resulting new segments.
   * See replaceSegments for recovery logic, in case the broker dies in the middle of this operation.
   * <p>Note that this method assumes we have already determined that the segment passed in contains records that cause
   * offset overflow.</p>
   * <p>The split logic overloads the use of .clean files that LogCleaner typically uses to make the process of replacing
   * the input segment with multiple new segments atomic and recoverable in the event of a crash. See replaceSegments
   * and completeSwapOperations for the implementation to make this operation recoverable on crashes.</p>
   * @param segment Segment to split
   * @return List of new segments that replace the input segment
   *
   * 将一个位移溢出的日志段分割成若干个日志段, 分割后的日志段不存在位移溢出的情况. 如果此方法执行成功, 那么输入的日志段将被删除,
   * 被返回的日志段所替代. 如果在执行过程中broker退出, 参考replaceSegments方法的恢复逻辑.
   */
  private[log] def splitOverflowedSegment(segment: LogSegment): List[LogSegment] = {
    //检查文件后缀是否为.log
    require(isLogFile(segment.log.file), s"Cannot split file ${segment.log.file.getAbsoluteFile}")

    //检查日志段是否位移溢出(最大的消息位移 - 日志段基准位移 > Int.MaxValue)
    require(segment.hasOverflow, "Split operation is only permitted for segments with overflow")

    info(s"Splitting overflowed segment $segment")

    //返回的切割后日志段列表
    val newSegments = ListBuffer[LogSegment]()

    //切割逻辑
    try {
      var position = 0
      val sourceRecords = segment.log

      //循环切割, 直至处理完原文件所有数据
      while (position < sourceRecords.sizeInBytes) {
        //第一个batch
        val firstBatch = sourceRecords.batchesFrom(position).asScala.head
        //创建新的日志段, 并把日志数据存放在一个.cleaned文件中
        val newSegment = LogCleaner.createNewCleanedSegment(this, firstBatch.baseOffset)
        newSegments += newSegment

        //从原文件中读取消息写入新的日志段, 直到位移溢出
        val bytesAppended = newSegment.appendFromFile(sourceRecords, position)
        if (bytesAppended == 0)
          throw new IllegalStateException(s"Failed to append records from position $position in $segment")

        position += bytesAppended
      }

      // prepare new segments
      var totalSizeOfNewSegments = 0
      newSegments.foreach { splitSegment =>
        //新的日志段文件磁盘
        splitSegment.onBecomeInactiveSegment()
        splitSegment.flush()
        splitSegment.lastModified = segment.lastModified
        totalSizeOfNewSegments += splitSegment.log.sizeInBytes
      }

      // size of all the new segments combined must equal size of the original segment
      //校验切割前后的日志大小
      if (totalSizeOfNewSegments != segment.log.sizeInBytes)
        throw new IllegalStateException("Inconsistent segment sizes after split" +
          s" before: ${segment.log.sizeInBytes} after: $totalSizeOfNewSegments")

      // replace old segment with new ones
      // 替换老的日志段文件
      info(s"Replacing overflowed segment $segment with split segments $newSegments")
      replaceSegments(newSegments.toList, List(segment), isRecoveredSwapFile = false)
      newSegments.toList
    } catch {
      case e: Exception =>
        newSegments.foreach { splitSegment =>
          splitSegment.close()
          splitSegment.deleteIfExists()
        }
        throw e
    }
  }
}

/**
 * Helper functions for logs
 */
object Log {

  /** a log file */
  //日志文件后缀
  val LogFileSuffix = ".log"

  /** an index file */
  //位移索引文件后缀
  val IndexFileSuffix = ".index"

  /** a time index file */
  //时间戳索引文件后缀
  val TimeIndexFileSuffix = ".timeindex"

  //生产者快照文件后缀
  val ProducerSnapshotFileSuffix = ".snapshot"

  /** an (aborted) txn index */
  //(已终止)事务索引文件后缀
  val TxnIndexFileSuffix = ".txnindex"

  /** a file that is scheduled to be deleted */
  //调度删除的文件后缀
  val DeletedFileSuffix = ".deleted"

  /** A temporary file that is being used for log cleaning */
  //用来做日志清理的临时文件后缀
  val CleanedFileSuffix = ".cleaned"

  /** A temporary file used when swapping files into the log */
  //用来做文件swap的临时文件
  val SwapFileSuffix = ".swap"

  /** Clean shutdown file that indicates the broker was cleanly shutdown in 0.8 and higher.
   * This is used to avoid unnecessary recovery after a clean shutdown. In theory this could be
   * avoided by passing in the recovery point, however finding the correct position to do this
   * requires accessing the offset index which may not be safe in an unclean shutdown.
   * For more information see the discussion in PR#2104
   *
   * 此文件在0.8版本或更高版本中用来标识broker是优雅退出的, 这样可以避免在优雅退出后重启时不必要的故障恢复.
   * 理论上也可以通过恢复点来实现, 但是故障恢复点依赖位移索引文件来查询正确的文件内物理偏移, 而在非优雅关机下
    * 索引文件可能也会被损坏.
   */
  val CleanShutdownFile = ".kafka_cleanshutdown"

  /** a directory that is scheduled to be deleted */
  //调度删除的目录后缀
  val DeleteDirSuffix = "-delete"

  /** a directory that is used for future partition */
  //计划未来用来做分区目录的目录后缀
  val FutureDirSuffix = "-future"

  private val DeleteDirPattern = Pattern.compile(s"^(\\S+)-(\\S+)\\.(\\S+)$DeleteDirSuffix")
  private val FutureDirPattern = Pattern.compile(s"^(\\S+)-(\\S+)\\.(\\S+)$FutureDirSuffix")

  val UnknownLogStartOffset = -1L

  def apply(dir: File,
            config: LogConfig,
            logStartOffset: Long,
            recoveryPoint: Long,
            scheduler: Scheduler,
            brokerTopicStats: BrokerTopicStats,
            time: Time = Time.SYSTEM,
            maxProducerIdExpirationMs: Int,
            producerIdExpirationCheckIntervalMs: Int,
            logDirFailureChannel: LogDirFailureChannel): Log = {
    //获取主题和分区信息
    val topicPartition = Log.parseTopicPartitionName(dir)
    //创建生产者状态管理者
    val producerStateManager = new ProducerStateManager(topicPartition, dir, maxProducerIdExpirationMs)
    //创建日志
    new Log(dir, config, logStartOffset, recoveryPoint, scheduler, brokerTopicStats, time, maxProducerIdExpirationMs,
      producerIdExpirationCheckIntervalMs, topicPartition, producerStateManager, logDirFailureChannel)
  }

  /**
   * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
   * so that ls sorts the files numerically.
   *
   * @param offset The offset to use in the file name
   * @return The filename
   */
  def filenamePrefixFromOffset(offset: Long): String = {
    val nf = NumberFormat.getInstance()
    nf.setMinimumIntegerDigits(20)
    nf.setMaximumFractionDigits(0)
    nf.setGroupingUsed(false)
    nf.format(offset)
  }

  /**
   * Construct a log file name in the given dir with the given base offset and the given suffix
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   * @param suffix The suffix to be appended to the file name (e.g. "", ".deleted", ".cleaned", ".swap", etc.)
   */
  def logFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + LogFileSuffix + suffix)

  /**
   * Return a directory name to rename the log directory to for async deletion. The name will be in the following
   * format: topic-partition.uniqueId-delete where topic, partition and uniqueId are variables.
   */
  def logDeleteDirName(topicPartition: TopicPartition): String = {
    logDirNameWithSuffix(topicPartition, DeleteDirSuffix)
  }

  /**
   * Return a future directory name for the given topic partition. The name will be in the following
   * format: topic-partition.uniqueId-future where topic, partition and uniqueId are variables.
   */
  def logFutureDirName(topicPartition: TopicPartition): String = {
    logDirNameWithSuffix(topicPartition, FutureDirSuffix)
  }

  private def logDirNameWithSuffix(topicPartition: TopicPartition, suffix: String): String = {
    val uniqueId = java.util.UUID.randomUUID.toString.replaceAll("-", "")
    s"${logDirName(topicPartition)}.$uniqueId$suffix"
  }

  /**
   * Return a directory name for the given topic partition. The name will be in the following
   * format: topic-partition where topic, partition are variables.
   */
  def logDirName(topicPartition: TopicPartition): String = {
    s"${topicPartition.topic}-${topicPartition.partition}"
  }

  /**
   * Construct an index file name in the given dir using the given base offset and the given suffix
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   * @param suffix The suffix to be appended to the file name ("", ".deleted", ".cleaned", ".swap", etc.)
   */
  def offsetIndexFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + IndexFileSuffix + suffix)

  /**
   * Construct a time index file name in the given dir using the given base offset and the given suffix
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   * @param suffix The suffix to be appended to the file name ("", ".deleted", ".cleaned", ".swap", etc.)
   */
  def timeIndexFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + TimeIndexFileSuffix + suffix)

  def deleteFileIfExists(file: File, suffix: String = ""): Unit =
    Files.deleteIfExists(new File(file.getPath + suffix).toPath)

  /**
   * Construct a producer id snapshot file using the given offset.
   *
   * @param dir The directory in which the log will reside
   * @param offset The last offset (exclusive) included in the snapshot
   */
  def producerSnapshotFile(dir: File, offset: Long): File =
    new File(dir, filenamePrefixFromOffset(offset) + ProducerSnapshotFileSuffix)

  /**
   * Construct a transaction index file name in the given dir using the given base offset and the given suffix
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   * @param suffix The suffix to be appended to the file name ("", ".deleted", ".cleaned", ".swap", etc.)
   */
  def transactionIndexFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + TxnIndexFileSuffix + suffix)

  //从文件名中获取位移, 取第一个.之前的字符串
  def offsetFromFileName(filename: String): Long = {
    filename.substring(0, filename.indexOf('.')).toLong
  }

  //从文件中获取位移
  def offsetFromFile(file: File): Long = {
    offsetFromFileName(file.getName)
  }

  /**
   * Calculate a log's size (in bytes) based on its log segments
   *
   * @param segments The log segments to calculate the size of
   * @return Sum of the log segments' sizes (in bytes)
   */
  def sizeInBytes(segments: Iterable[LogSegment]): Long =
    segments.map(_.size.toLong).sum

  /**
   * Parse the topic and partition out of the directory name of a log
   * 根据日志的目录名称获取主题和分区
   */
  def parseTopicPartitionName(dir: File): TopicPartition = {
    if (dir == null)
      throw new KafkaException("dir should not be null")

    def exception(dir: File): KafkaException = {
      new KafkaException(s"Found directory ${dir.getCanonicalPath}, '${dir.getName}' is not in the form of " +
        "topic-partition or topic-partition.uniqueId-delete (if marked for deletion).\n" +
        "Kafka's log directories (and children) should only contain Kafka topic data.")
    }

    val dirName = dir.getName
    if (dirName == null || dirName.isEmpty || !dirName.contains('-'))
      throw exception(dir)
    if (dirName.endsWith(DeleteDirSuffix) && !DeleteDirPattern.matcher(dirName).matches ||
        dirName.endsWith(FutureDirSuffix) && !FutureDirPattern.matcher(dirName).matches)
      throw exception(dir)

    val name: String =
      if (dirName.endsWith(DeleteDirSuffix) || dirName.endsWith(FutureDirSuffix)) dirName.substring(0, dirName.lastIndexOf('.'))
      else dirName

    val index = name.lastIndexOf('-')
    val topic = name.substring(0, index)
    val partitionString = name.substring(index + 1)
    if (topic.isEmpty || partitionString.isEmpty)
      throw exception(dir)

    val partition =
      try partitionString.toInt
      catch { case _: NumberFormatException => throw exception(dir) }

    new TopicPartition(topic, partition)
  }

  //判断file是否为索引文件, 索引文件有三种: 1)位移索引; 2)时间索引; 3)事务索引
  private def isIndexFile(file: File): Boolean = {
    val filename = file.getName
    filename.endsWith(IndexFileSuffix) || filename.endsWith(TimeIndexFileSuffix) || filename.endsWith(TxnIndexFileSuffix)
  }

  private def isLogFile(file: File): Boolean =
    file.getPath.endsWith(LogFileSuffix)

}

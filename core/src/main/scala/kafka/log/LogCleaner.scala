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
import java.nio._
import java.util.Date
import java.util.concurrent.TimeUnit

import com.yammer.metrics.core.Gauge
import kafka.common._
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{BrokerReconfigurable, KafkaConfig, LogDirFailureChannel}
import kafka.utils._
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.{CorruptRecordException, KafkaStorageException}
import org.apache.kafka.common.record.MemoryRecords.RecordFilter
import org.apache.kafka.common.record.MemoryRecords.RecordFilter.BatchRetention
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.collection.{Set, mutable}

/**
 * The cleaner is responsible for removing obsolete records from logs which have the "compact" retention strategy.
 * A message with key K and offset O is obsolete if there exists a message with key K and offset O' such that O < O'.
 *
 * Each log can be thought of being split into two sections of segments: a "clean" section which has previously been cleaned followed by a
 * "dirty" section that has not yet been cleaned. The dirty section is further divided into the "cleanable" section followed by an "uncleanable" section.
 * The uncleanable section is excluded from cleaning. The active log segment is always uncleanable. If there is a
 * compaction lag time set, segments whose largest message timestamp is within the compaction lag time of the cleaning operation are also uncleanable.
 *
 * The cleaning is carried out by a pool of background threads. Each thread chooses the dirtiest log that has the "compact" retention policy
 * and cleans that. The dirtiness of the log is guessed by taking the ratio of bytes in the dirty section of the log to the total bytes in the log.
 *
 * To clean a log the cleaner first builds a mapping of key=>last_offset for the dirty section of the log. See kafka.log.OffsetMap for details of
 * the implementation of the mapping.
 *
 * Once the key=>last_offset map is built, the log is cleaned by recopying each log segment but omitting any key that appears in the offset map with a
 * higher offset than what is found in the segment (i.e. messages with a key that appears in the dirty section of the log).
 *
 * To avoid segments shrinking to very small sizes with repeated cleanings we implement a rule by which if we will merge successive segments when
 * doing a cleaning if their log and index size are less than the maximum log and index size prior to the clean beginning.
 *
 * Cleaned segments are swapped into the log as they become available.
 *
 * One nuance that the cleaner must handle is log truncation. If a log is truncated while it is being cleaned the cleaning of that log is aborted.
 *
 * Messages with null payload are treated as deletes for the purpose of log compaction. This means that they receive special treatment by the cleaner.
 * The cleaner will only retain delete records for a period of time to avoid accumulating space indefinitely. This period of time is configurable on a per-topic
 * basis and is measured from the time the segment enters the clean portion of the log (at which point any prior message with that key has been removed).
 * Delete markers in the clean section of the log that are older than this time will not be retained when log segments are being recopied as part of cleaning.
 *
 * Note that cleaning is more complicated with the idempotent/transactional producer capabilities. The following
 * are the key points:
 *
 * 1. In order to maintain sequence number continuity for active producers, we always retain the last batch
 *    from each producerId, even if all the records from the batch have been removed. The batch will be removed
 *    once the producer either writes a new batch or is expired due to inactivity.
 * 2. We do not clean beyond the last stable offset. This ensures that all records observed by the cleaner have
 *    been decided (i.e. committed or aborted). In particular, this allows us to use the transaction index to
 *    collect the aborted transactions ahead of time.
 * 3. Records from aborted transactions are removed by the cleaner immediately without regard to record keys.
 * 4. Transaction markers are retained until all record batches from the same transaction have been removed and
 *    a sufficient amount of time has passed to reasonably ensure that an active consumer wouldn't consume any
 *    data from the transaction prior to reaching the offset of the marker. This follows the same logic used for
 *    tombstone deletion.
 *
 * @param initialConfig Initial configuration parameters for the cleaner. Actual config may be dynamically updated.
 * @param logDirs The directories where offset checkpoints reside
 * @param logs The pool of logs
 * @param time A way to control the passage of time
 *
 * cleaner负责删除使用"compact"保留策略的过期日志记录. 对于使用"compact"日志保留策略的主题来说, 一个key为K且位移为O的消息过期,
 * 当且仅当存在一个key为K位移为O'而且O<O'的消息.
 *
 * 每个日志都可以分成两部分日志段: 第一部分为"干净"的, 这一部分已经被清理过; 紧接着的一部分为"脏"的, 这一部分没有被清理过.
 * 脏的部分可以进一步分成"可清理"和"不可清理". 不可清理的部分被排除在清理流程之外. 当前在追加消息的日志段总是不可清理的, 而且
 * 如果设置了compact的延迟时间, 那么在清理时消息最大时间戳在延迟时间内的日志段也是不可清理的.
 *
 * 清理工作有一个后台线程池来完成. 每个线程选择设置为"comapct"的最"脏"的日志来清理. 一个日志脏的程度由其脏的部分占日志比例来决定.
 *
 * 为了清理一个日志, cleaner首先对脏日志部分建立一个(键 => 最后位移)的映射, 具体实现详见kafka.log.OffsetMap.
 *
 * 一旦建立了(键 => 最后位移)的映射, 就可以复制日志段并抛弃那些键在映射中但位移更小的消息, 通过这样来完成脏日志的清理.
 *
 * 为了避免由于不断的清理而导致日志段越来越小, 我们实现了一个规则, 这个规则会在清理过程合并相邻的日志段, 如果这些日志段的日志和索引
 * 的大小在还没有开始清理时就小于最大的大小上限.
 *
 * 当已经清理的日志段可用时, 会被swap回日志.
 *
 * cleaner必须处理的一个小地方是日志截断. 如果一个日志在清理过程中被截断了, 那么日志清理会终止.
 *
 * 在compact日志中, payload为null的消息会被认为是需要删除的消息. 也就是说这些消息会被cleaner特殊对待. cleaner只会保留这些消息一段
 * 时间以避免占用太多空间. 这个时间是可以配置的, 而且每个主题都可以不同. 时间从日志段进入"干净"部分(也就是任何key和这个特殊消息的key相
 * 同的消息都被删除了)开始算起. 在干净日志中的这些删除标记超过配置的时间后, 在下一次日志段被复制时会被忽略(也就是真正的物理删除).
 *
 * 值得注意的是, 如果把生产者的幂等性/事务性考虑进来的话, 那么清理逻辑会变得更复杂. 下面是一些关键点:
 *
 * 1. 为了保持活跃生产者的序列号连续性, 对于每个生产者ID只会保留最后的消息batch, 即使这个batch中所有的记录已经被删除. 如果这个
 *    生产者写入新的batch或者由于不活跃而过期了, 会删除这个batch.
 * 2. 我们不会清理超过最后stable位移的消息. 这保证了cleaner所看到的所有记录状态都是确定的(已提交或已回滚). 而且, 这样做可以使用
 *    事务索引来提前收集已回滚事务.
 * 3. 已回滚事务会直接被cleaner删除, 而不会考虑它的键.
 * 4. 事务标记会一直保留直到事务中所有的记录都已经被删除了而且当前已经过去一段比较长的时间, 这样可以保证一个活跃的消费者不会消费任何
 *    在该事务标记前的属于同一个事务的记录数据. 这个逻辑和老日志删除的逻辑是相同的.
 *
 * 参数 initialConfig: cleaner的初始配置参数, 真正使用的配置可以被动态更新
 * 参数 logDirs: 位移检查点所在的目录
 * 参数 logs: 日志池
 * 参数 time: 用来获取时间的实例
 *
 */
class LogCleaner(initialConfig: CleanerConfig,
                 val logDirs: Seq[File],
                 val logs: Pool[TopicPartition, Log],
                 val logDirFailureChannel: LogDirFailureChannel,
                 time: Time = Time.SYSTEM) extends Logging with KafkaMetricsGroup with BrokerReconfigurable
{

  /* Log cleaner configuration which may be dynamically updated */
  // cleaner的配置, 可以被动态更新
  @volatile private var config = initialConfig

  /* for managing the state of partitions being cleaned. package-private to allow access in tests */
  // 用来在清理过程中管理分区状态
  private[log] val cleanerManager = new LogCleanerManager(logDirs, logs, logDirFailureChannel)

  /* a throttle used to limit the I/O of all the cleaner threads to a user-specified maximum rate */
  //一个用来限制所有清理线程IO速率的节流阀, 使用者可以指定最大速率
  private val throttler = new Throttler(desiredRatePerSec = config.maxIoBytesPerSecond,
                                        checkIntervalMs = 300,
                                        throttleDown = true,
                                        "cleaner-io",
                                        "bytes",
                                        time = time)

  /* the threads */
  //清理线程
  private val cleaners = mutable.ArrayBuffer[CleanerThread]()

  /* a metric to track the maximum utilization of any thread's buffer in the last cleaning */
  //记录上一次清理时所有线程缓冲区最大的使用率
  newGauge("max-buffer-utilization-percent",
           new Gauge[Int] {
             def value: Int = cleaners.map(_.lastStats).map(100 * _.bufferUtilization).max.toInt
           })
  /* a metric to track the recopy rate of each thread's last cleaning */
  // 跟踪每个线程上一次清理时的复制率
  newGauge("cleaner-recopy-percent",
           new Gauge[Int] {
             def value: Int = {
               val stats = cleaners.map(_.lastStats)
               val recopyRate = stats.map(_.bytesWritten).sum.toDouble / math.max(stats.map(_.bytesRead).sum, 1)
               (100 * recopyRate).toInt
             }
           })
  /* a metric to track the maximum cleaning time for the last cleaning from each thread */
  // 用来跟踪上一次清理时线程最大的清理时间
  newGauge("max-clean-time-secs",
           new Gauge[Int] {
             def value: Int = cleaners.map(_.lastStats).map(_.elapsedSecs).max.toInt
           })

  /**
   * Start the background cleaning
   * 启动后台清理线程
   */
  def startup() {
    info("Starting the log cleaner")
    (0 until config.numThreads).foreach { i =>
      val cleaner = new CleanerThread(i)
      cleaners += cleaner
      cleaner.start()
    }
  }

  /**
   * Stop the background cleaning
   */
  def shutdown() {
    info("Shutting down the log cleaner.")
    cleaners.foreach(_.shutdown())
    cleaners.clear()
  }

  override def reconfigurableConfigs: Set[String] = {
    LogCleaner.ReconfigurableConfigs
  }

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    val newCleanerConfig = LogCleaner.cleanerConfig(newConfig)
    val numThreads = newCleanerConfig.numThreads
    val currentThreads = config.numThreads
    if (numThreads < 1)
      throw new ConfigException(s"Log cleaner threads should be at least 1")
    if (numThreads < currentThreads / 2)
      throw new ConfigException(s"Log cleaner threads cannot be reduced to less than half the current value $currentThreads")
    if (numThreads > currentThreads * 2)
      throw new ConfigException(s"Log cleaner threads cannot be increased to more than double the current value $currentThreads")

  }

  /**
    * Reconfigure log clean config. This simply stops current log cleaners and creates new ones.
    * That ensures that if any of the cleaners had failed, new cleaners are created to match the new config.
    */
  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    config = LogCleaner.cleanerConfig(newConfig)
    shutdown()
    startup()
  }

  /**
   *  Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
   *  the partition is aborted.
   */
  def abortCleaning(topicPartition: TopicPartition) {
    cleanerManager.abortCleaning(topicPartition)
  }

  /**
   * Update checkpoint file, removing topics and partitions that no longer exist
   */
  def updateCheckpoints(dataDir: File) {
    cleanerManager.updateCheckpoints(dataDir, update=None)
  }

  def alterCheckpointDir(topicPartition: TopicPartition, sourceLogDir: File, destLogDir: File): Unit = {
    cleanerManager.alterCheckpointDir(topicPartition, sourceLogDir, destLogDir)
  }

  def handleLogDirFailure(dir: String) {
    cleanerManager.handleLogDirFailure(dir)
  }

  /**
   * Truncate cleaner offset checkpoint for the given partition if its checkpointed offset is larger than the given offset
   */
  def maybeTruncateCheckpoint(dataDir: File, topicPartition: TopicPartition, offset: Long) {
    cleanerManager.maybeTruncateCheckpoint(dataDir, topicPartition, offset)
  }

  /**
   *  Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
   *  This call blocks until the cleaning of the partition is aborted and paused.
   */
  def abortAndPauseCleaning(topicPartition: TopicPartition) {
    cleanerManager.abortAndPauseCleaning(topicPartition)
  }

  /**
   *  Resume the cleaning of a paused partition. This call blocks until the cleaning of a partition is resumed.
   */
  def resumeCleaning(topicPartition: TopicPartition) {
    cleanerManager.resumeCleaning(topicPartition)
  }

  /**
   * For testing, a way to know when work has completed. This method waits until the
   * cleaner has processed up to the given offset on the specified topic/partition
   *
   * @param topicPartition The topic and partition to be cleaned
   * @param offset The first dirty offset that the cleaner doesn't have to clean
   * @param maxWaitMs The maximum time in ms to wait for cleaner
   *
   * @return A boolean indicating whether the work has completed before timeout
   */
  def awaitCleaned(topicPartition: TopicPartition, offset: Long, maxWaitMs: Long = 60000L): Boolean = {
    def isCleaned = cleanerManager.allCleanerCheckpoints.get(topicPartition).fold(false)(_ >= offset)
    var remainingWaitMs = maxWaitMs
    while (!isCleaned && remainingWaitMs > 0) {
      val sleepTime = math.min(100, remainingWaitMs)
      Thread.sleep(sleepTime)
      remainingWaitMs -= sleepTime
    }
    isCleaned
  }

  // Only for testing
  private[kafka] def currentConfig: CleanerConfig = config

  // Only for testing
  private[log] def cleanerCount: Int = cleaners.size

  /**
   * The cleaner threads do the actual log cleaning. Each thread processes does its cleaning repeatedly by
   * choosing the dirtiest log, cleaning it, and then swapping in the cleaned segments.
   *
   * 用来真正做日志清理的清理线程. 每个线程不断重复的进行如下操作:
   * 1) 选择最脏的日志;
   * 2) 清理日志;
   * 3) swap已经清理的日志段
   */
  private class CleanerThread(threadId: Int)
    extends ShutdownableThread(name = "kafka-log-cleaner-thread-" + threadId, isInterruptible = false) {

    protected override def loggerName = classOf[LogCleaner].getName

    if (config.dedupeBufferSize / config.numThreads > Int.MaxValue)
      warn("Cannot use more than 2G of cleaner buffer space per cleaner thread, ignoring excess buffer space...")

    val cleaner = new Cleaner(id = threadId,
                              offsetMap = new SkimpyOffsetMap(memory = math.min(config.dedupeBufferSize / config.numThreads, Int.MaxValue).toInt,
                                                              hashAlgorithm = config.hashAlgorithm),
                              ioBufferSize = config.ioBufferSize / config.numThreads / 2,
                              maxIoBufferSize = config.maxMessageSize,
                              dupBufferLoadFactor = config.dedupeBufferLoadFactor,
                              throttler = throttler,
                              time = time,
                              checkDone = checkDone)

    @volatile var lastStats: CleanerStats = new CleanerStats()

    private def checkDone(topicPartition: TopicPartition) {
      if (!isRunning)
        throw new ThreadShutdownException
      cleanerManager.checkCleaningAborted(topicPartition)
    }

    /**
     * The main loop for the cleaner thread
     * 每个清理线程的主循环方法
     */
    override def doWork() {
      cleanOrSleep()
    }

    /**
     * Clean a log if there is a dirty log available, otherwise sleep for a bit
     * 如果存在一个脏日志则清理它, 否则睡眠一段时间
     */
    private def cleanOrSleep() {
      val cleaned = cleanerManager.grabFilthiestCompactedLog(time) match {
        case None =>
          false
        case Some(cleanable) =>
          // there's a log, clean it
          // 找到可清理的日志, 则清理它
          var endOffset = cleanable.firstDirtyOffset
          try {
            val (nextDirtyOffset, cleanerStats) = cleaner.clean(cleanable)
            recordStats(cleaner.id, cleanable.log.name, cleanable.firstDirtyOffset, endOffset, cleanerStats)
            endOffset = nextDirtyOffset
          } catch {
            case _: LogCleaningAbortedException => // task can be aborted, let it go.
            case _: KafkaStorageException => // partition is already offline. let it go.
            case e: IOException =>
              val msg = s"Failed to clean up log for ${cleanable.topicPartition} in dir ${cleanable.log.dir.getParent} due to IOException"
              logDirFailureChannel.maybeAddOfflineLogDir(cleanable.log.dir.getParent, msg, e)
          } finally {
            cleanerManager.doneCleaning(cleanable.topicPartition, cleanable.log.dir.getParentFile, endOffset)
          }
          true
      }
      val deletable: Iterable[(TopicPartition, Log)] = cleanerManager.deletableLogs()
      deletable.foreach{
        case (topicPartition, log) =>
          try {
            log.deleteOldSegments()
          } finally {
            cleanerManager.doneDeleting(topicPartition)
          }
      }
      if (!cleaned)
        pause(config.backOffMs, TimeUnit.MILLISECONDS)
    }

    /**
     * Log out statistics on a single run of the cleaner.
     */
    def recordStats(id: Int, name: String, from: Long, to: Long, stats: CleanerStats) {
      this.lastStats = stats
      def mb(bytes: Double) = bytes / (1024*1024)
      val message =
        "%n\tLog cleaner thread %d cleaned log %s (dirty section = [%d, %d])%n".format(id, name, from, to) +
        "\t%,.1f MB of log processed in %,.1f seconds (%,.1f MB/sec).%n".format(mb(stats.bytesRead),
                                                                                stats.elapsedSecs,
                                                                                mb(stats.bytesRead/stats.elapsedSecs)) +
        "\tIndexed %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n".format(mb(stats.mapBytesRead),
                                                                                           stats.elapsedIndexSecs,
                                                                                           mb(stats.mapBytesRead)/stats.elapsedIndexSecs,
                                                                                           100 * stats.elapsedIndexSecs/stats.elapsedSecs) +
        "\tBuffer utilization: %.1f%%%n".format(100 * stats.bufferUtilization) +
        "\tCleaned %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n".format(mb(stats.bytesRead),
                                                                                           stats.elapsedSecs - stats.elapsedIndexSecs,
                                                                                           mb(stats.bytesRead)/(stats.elapsedSecs - stats.elapsedIndexSecs), 100 * (stats.elapsedSecs - stats.elapsedIndexSecs).toDouble/stats.elapsedSecs) +
        "\tStart size: %,.1f MB (%,d messages)%n".format(mb(stats.bytesRead), stats.messagesRead) +
        "\tEnd size: %,.1f MB (%,d messages)%n".format(mb(stats.bytesWritten), stats.messagesWritten) +
        "\t%.1f%% size reduction (%.1f%% fewer messages)%n".format(100.0 * (1.0 - stats.bytesWritten.toDouble/stats.bytesRead),
                                                                   100.0 * (1.0 - stats.messagesWritten.toDouble/stats.messagesRead))
      info(message)
      if (stats.invalidMessagesRead > 0) {
        warn("\tFound %d invalid messages during compaction.".format(stats.invalidMessagesRead))
      }
    }

  }
}

object LogCleaner {
  val ReconfigurableConfigs = Set(
    KafkaConfig.LogCleanerThreadsProp,
    KafkaConfig.LogCleanerDedupeBufferSizeProp,
    KafkaConfig.LogCleanerDedupeBufferLoadFactorProp,
    KafkaConfig.LogCleanerIoBufferSizeProp,
    KafkaConfig.MessageMaxBytesProp,
    KafkaConfig.LogCleanerIoMaxBytesPerSecondProp,
    KafkaConfig.LogCleanerBackoffMsProp
  )

  def cleanerConfig(config: KafkaConfig): CleanerConfig = {
    CleanerConfig(numThreads = config.logCleanerThreads,
      dedupeBufferSize = config.logCleanerDedupeBufferSize,
      dedupeBufferLoadFactor = config.logCleanerDedupeBufferLoadFactor,
      ioBufferSize = config.logCleanerIoBufferSize,
      maxMessageSize = config.messageMaxBytes,
      maxIoBytesPerSecond = config.logCleanerIoMaxBytesPerSecond,
      backOffMs = config.logCleanerBackoffMs,
      enableCleaner = config.logCleanerEnable)

  }

  def createNewCleanedSegment(log: Log, baseOffset: Long): LogSegment = {
    LogSegment.deleteIfExists(log.dir, baseOffset, fileSuffix = Log.CleanedFileSuffix)

    //创建一个新的日志段
    LogSegment.open(log.dir, baseOffset, log.config, Time.SYSTEM, fileAlreadyExists = false,
      fileSuffix = Log.CleanedFileSuffix, initFileSize = log.initFileSize, preallocate = log.config.preallocate)
  }
}

/**
 * This class holds the actual logic for cleaning a log
 * @param id An identifier used for logging
 * @param offsetMap The map used for deduplication
 * @param ioBufferSize The size of the buffers to use. Memory usage will be 2x this number as there is a read and write buffer.
 * @param maxIoBufferSize The maximum size of a message that can appear in the log
 * @param dupBufferLoadFactor The maximum percent full for the deduplication buffer
 * @param throttler The throttler instance to use for limiting I/O rate.
 * @param time The time instance
 * @param checkDone Check if the cleaning for a partition is finished or aborted.
 *
 * 此类包含清理日志的底层逻辑
 * 参数 id: 用于清理的标识符
 * 参数 offsetMap: 用于去重的位移map
 * 参数 ioBufferSize: 缓冲区的大小. 内存会使用此大小的2倍, 因为有读缓冲区和写缓冲区
 * 参数 maxIoBufferSize: 消息可以出现在日志中的最大大小
 * 参数 dupBufferLoadFactor: 去重缓冲区的满载因子
 * 参数 throttler: 用来限制IO速率的节流阀
 * 参数 time: 用来获取时间的实例
 * 参数 checkDone: 检查一个分区的清理是否完成或被终止
 *
 */
private[log] class Cleaner(val id: Int,
                           val offsetMap: OffsetMap,
                           ioBufferSize: Int,
                           maxIoBufferSize: Int,
                           dupBufferLoadFactor: Double,
                           throttler: Throttler,
                           time: Time,
                           checkDone: (TopicPartition) => Unit) extends Logging {

  protected override def loggerName = classOf[LogCleaner].getName

  this.logIdent = "Cleaner " + id + ": "

  /* buffer used for read i/o */
  private var readBuffer = ByteBuffer.allocate(ioBufferSize)

  /* buffer used for write i/o */
  private var writeBuffer = ByteBuffer.allocate(ioBufferSize)

  private val decompressionBufferSupplier = BufferSupplier.create();

  require(offsetMap.slots * dupBufferLoadFactor > 1, "offset map is too small to fit in even a single message, so log cleaning will never make progress. You can increase log.cleaner.dedupe.buffer.size or decrease log.cleaner.threads")

  /**
   * Clean the given log
   *
   * @param cleanable The log to be cleaned
   *
   * @return The first offset not cleaned and the statistics for this round of cleaning
   *
   * 清理指定的日志
   *
   * 参数 cleanable: 需要进行清理的日志
   * 返回 未清理的初始位移, 以及此次清理的指标统计
   */
  private[log] def clean(cleanable: LogToClean): (Long, CleanerStats) = {
    // figure out the timestamp below which it is safe to remove delete tombstones
    // this position is defined to be a configurable time beneath the last modified time of the last clean segment
    // 计算安全移除删除标记的时间点, 这个时间点定义为 (最后一个干净日志段最后修改时间 - 配置的延后时间)

    val deleteHorizonMs =
      cleanable.log.logSegments(0, cleanable.firstDirtyOffset).lastOption match {
        case None => 0L
        case Some(seg) => seg.lastModified - cleanable.log.config.deleteRetentionMs
    }

    doClean(cleanable, deleteHorizonMs)
  }

  private[log] def doClean(cleanable: LogToClean, deleteHorizonMs: Long): (Long, CleanerStats) = {
    info("Beginning cleaning of log %s.".format(cleanable.log.name))

    val log = cleanable.log
    val stats = new CleanerStats()

    // build the offset map
    // 构建key与offset映射的map
    info("Building offset map for %s...".format(cleanable.log.name))
    val upperBoundOffset = cleanable.firstUncleanableOffset
    buildOffsetMap(log, cleanable.firstDirtyOffset, upperBoundOffset, offsetMap, stats)
    val endOffset = offsetMap.latestOffset + 1
    stats.indexDone()

    // determine the timestamp up to which the log will be cleaned
    // this is the lower of the last active segment and the compaction lag
    // 计算清理日志的时间戳上限, 这是当前使用日志段的前一个日志段和压缩延后时间的最小值
    val cleanableHorizonMs = log.logSegments(0, cleanable.firstUncleanableOffset).lastOption.map(_.lastModified).getOrElse(0L)

    // group the segments and clean the groups
    // 将日志段分组并按分组清理
    info("Cleaning log %s (cleaning prior to %s, discarding tombstones prior to %s)...".format(log.name, new Date(cleanableHorizonMs), new Date(deleteHorizonMs)))
    for (group <- groupSegmentsBySize(log.logSegments(0, endOffset), log.config.segmentSize, log.config.maxIndexSize, cleanable.firstUncleanableOffset))
      cleanSegments(log, group, offsetMap, deleteHorizonMs, stats)

    // record buffer utilization
    stats.bufferUtilization = offsetMap.utilization

    stats.allDone()

    (endOffset, stats)
  }

  /**
   * Clean a group of segments into a single replacement segment
   *
   * @param log The log being cleaned
   * @param segments The group of segments being cleaned
   * @param map The offset map to use for cleaning segments
   * @param deleteHorizonMs The time to retain delete tombstones
   * @param stats Collector for cleaning statistics
   *
   * 清理一组日志段变成一个日志段
   *
   * 参数 log: 待清理的日志
   * 参数 segments: 待清理的日志段分组
   * 参数 map: 用来清理日志段的offset map
   * 参数 deleteHorizonMs: 用来获取遗留标记删除时间点
   * 参数 stats: 用来收集清理统计指标
   */
  private[log] def cleanSegments(log: Log,
                                 segments: Seq[LogSegment],
                                 map: OffsetMap,
                                 deleteHorizonMs: Long,
                                 stats: CleanerStats) {
    // create a new segment with a suffix appended to the name of the log and indexes
    // 创建一个新的日志段, 该日志段的日志文件和索引文件都有一个特殊.cleaned后缀
    val cleaned = LogCleaner.createNewCleanedSegment(log, segments.head.baseOffset)

    try {
      // clean segments into the new destination segment
      // 清理日志段并写入到新的目标日志段

      val iter = segments.iterator
      var currentSegmentOpt: Option[LogSegment] = Some(iter.next())
      while (currentSegmentOpt.isDefined) {
        val currentSegment = currentSegmentOpt.get
        val nextSegmentOpt = if (iter.hasNext) Some(iter.next()) else None

        //获取已回滚事务
        val startOffset = currentSegment.baseOffset
        val upperBoundOffset = nextSegmentOpt.map(_.baseOffset).getOrElse(map.latestOffset + 1)
        val abortedTransactions = log.collectAbortedTransactions(startOffset, upperBoundOffset)
        val transactionMetadata = CleanedTransactionMetadata(abortedTransactions, Some(cleaned.txnIndex))

        //是否保留删除标记
        val retainDeletes = currentSegment.lastModified > deleteHorizonMs
        info(s"Cleaning segment $startOffset in log ${log.name} (largest timestamp ${new Date(currentSegment.largestTimestamp)}) " +
          s"into ${cleaned.baseOffset}, ${if(retainDeletes) "retaining" else "discarding"} deletes.")

        try {
          //清理日志段并写入新的日志段
          cleanInto(log.topicPartition, currentSegment.log, cleaned, map, retainDeletes, log.config.maxMessageSize,
            transactionMetadata, log.activeProducersWithLastSequence, stats)
        } catch {
          case e: LogSegmentOffsetOverflowException =>
            // Split the current segment. It's also safest to abort the current cleaning process, so that we retry from
            // scratch once the split is complete.
            info(s"Caught segment overflow error during cleaning: ${e.getMessage}")
            log.splitOverflowedSegment(currentSegment)
            throw new LogCleaningAbortedException()
        }
        currentSegmentOpt = nextSegmentOpt
      }

      cleaned.onBecomeInactiveSegment()
      // flush new segment to disk before swap
      cleaned.flush()

      // update the modification date to retain the last modified date of the original files
      val modified = segments.last.lastModified
      cleaned.lastModified = modified

      // swap in new segment
      info(s"Swapping in cleaned segment $cleaned for segment(s) $segments in log $log")
      log.replaceSegments(List(cleaned), segments)
    } catch {
      case e: LogCleaningAbortedException =>
        try cleaned.deleteIfExists()
        catch {
          case deleteException: Exception =>
            e.addSuppressed(deleteException)
        } finally throw e
    }
  }

  /**
   * Clean the given source log segment into the destination segment using the key=>offset mapping
   * provided
   *
   * @param topicPartition The topic and partition of the log segment to clean
   * @param sourceRecords The dirty log segment
   * @param dest The cleaned log segment
   * @param map The key=>offset mapping
   * @param retainDeletes Should delete tombstones be retained while cleaning this segment
   * @param maxLogMessageSize The maximum message size of the corresponding topic
   * @param stats Collector for cleaning statistics
   *
   * 使用参数中的key=>offset映射清理指定日志段并写入目标日志段
   *
   * 参数 topicPartition: 清理的日志段所属的主题和分区
   * 参数 sourceRecords: 脏日志段
   * 参数 dest: 目标日志段
   * 参数 map: key=>offset映射
   * 参数 retainDeletes: 在清理日志段过程中是否保留删除标记
   * 参数 maxLogMessageSize: 主题的最大消息大小
   * 参数 stats: 收集清理统计指标
   */
  private[log] def cleanInto(topicPartition: TopicPartition,
                             sourceRecords: FileRecords,
                             dest: LogSegment,
                             map: OffsetMap,
                             retainDeletes: Boolean,
                             maxLogMessageSize: Int,
                             transactionMetadata: CleanedTransactionMetadata,
                             activeProducers: Map[Long, Int],
                             stats: CleanerStats) {
    val logCleanerFilter = new RecordFilter {
      var discardBatchRecords: Boolean = _

      override def checkBatchRetention(batch: RecordBatch): BatchRetention = {
        // we piggy-back on the tombstone retention logic to delay deletion of transaction markers.
        // note that we will never delete a marker until all the records from that transaction are removed.
        discardBatchRecords = shouldDiscardBatch(batch, transactionMetadata, retainTxnMarkers = retainDeletes)

        // check if the batch contains the last sequence number for the producer. if so, we cannot
        // remove the batch just yet or the producer may see an out of sequence error.
        if (batch.hasProducerId && activeProducers.get(batch.producerId).contains(batch.lastSequence))
          BatchRetention.RETAIN_EMPTY
        else if (discardBatchRecords)
          BatchRetention.DELETE
        else
          BatchRetention.DELETE_EMPTY
      }

      override def shouldRetainRecord(batch: RecordBatch, record: Record): Boolean = {
        if (discardBatchRecords)
          // The batch is only retained to preserve producer sequence information; the records can be removed
          false
        else
          Cleaner.this.shouldRetainRecord(map, retainDeletes, batch, record, stats)
      }
    }

    var position = 0
    while (position < sourceRecords.sizeInBytes) {
      checkDone(topicPartition)
      // read a chunk of messages and copy any that are to be retained to the write buffer to be written out
      // 读取一段消息, 将需要保留的消息写入到写缓冲区
      readBuffer.clear()
      writeBuffer.clear()

      //从原日志段读取消息到读缓冲区
      sourceRecords.readInto(readBuffer, position)

      //转换成内存消息记录
      val records = MemoryRecords.readableRecords(readBuffer)

      //使用节流阀限速
      throttler.maybeThrottle(records.sizeInBytes)

      //过滤记录并写入到指定的缓冲区
      val result = records.filterTo(topicPartition, logCleanerFilter, writeBuffer, maxLogMessageSize, decompressionBufferSupplier)
      stats.readMessages(result.messagesRead, result.bytesRead)
      stats.recopyMessages(result.messagesRetained, result.bytesRetained)

      position += result.bytesRead

      // if any messages are to be retained, write them out
      val outputBuffer = result.output
      if (outputBuffer.position() > 0) {
        outputBuffer.flip()
        val retained = MemoryRecords.readableRecords(outputBuffer)
        // it's OK not to hold the Log's lock in this case, because this segment is only accessed by other threads
        // after `Log.replaceSegments` (which acquires the lock) is called
        dest.append(largestOffset = result.maxOffset,
          largestTimestamp = result.maxTimestamp,
          shallowOffsetOfMaxTimestamp = result.shallowOffsetOfMaxTimestamp,
          records = retained)
        throttler.maybeThrottle(outputBuffer.limit())
      }

      // if we read bytes but didn't get even one complete batch, our I/O buffer is too small, grow it and try again
      // `result.bytesRead` contains bytes from `messagesRead` and any discarded batches.
      if (readBuffer.limit() > 0 && result.bytesRead == 0)
        growBuffersOrFail(sourceRecords, position, maxLogMessageSize, records)
    }
    restoreBuffers()
  }


  /**
   * Grow buffers to process next batch of records from `sourceRecords.` Buffers are doubled in size
   * up to a maximum of `maxLogMessageSize`. In some scenarios, a record could be bigger than the
   * current maximum size configured for the log. For example:
   *   1. A compacted topic using compression may contain a message set slightly larger than max.message.bytes
   *   2. max.message.bytes of a topic could have been reduced after writing larger messages
   * In these cases, grow the buffer to hold the next batch.
   *
   * 增大缓冲区以处理`sourceRecords`的下一个batch记录. 缓冲区会以成倍增长直至达到`maxLogMessageSize`指定的上限.
   * 在一些场景下, 一个记录可能会比当前日志配置的最大值还要打. 例如:
   *   1. 一个配置为compact的主题压缩后可能会包含比max.message.bytes还稍微大一点的消息集
   *   2. 主题的max.message.bytes可能在写完大消息之后被更新变小了
   * 对于这些情况, 增大缓冲区以存放消息batch
   */
  private def growBuffersOrFail(sourceRecords: FileRecords,
                                position: Int,
                                maxLogMessageSize: Int,
                                memoryRecords: MemoryRecords): Unit = {

    val maxSize = if (readBuffer.capacity >= maxLogMessageSize) {
      //这里通过头部来获取batch的大小
      val nextBatchSize = memoryRecords.firstBatchSize
      val logDesc = s"log segment ${sourceRecords.file} at position $position"
      //以下做一些校验
      if (nextBatchSize == null)
        throw new IllegalStateException(s"Could not determine next batch size for $logDesc")
      if (nextBatchSize <= 0)
        throw new IllegalStateException(s"Invalid batch size $nextBatchSize for $logDesc")
      if (nextBatchSize <= readBuffer.capacity)
        throw new IllegalStateException(s"Batch size $nextBatchSize < buffer size ${readBuffer.capacity}, but not processed for $logDesc")
      val bytesLeft = sourceRecords.channel.size - position
      if (nextBatchSize > bytesLeft)
        throw new CorruptRecordException(s"Log segment may be corrupt, batch size $nextBatchSize > $bytesLeft bytes left in segment for $logDesc")
      //用头部中的大小作为缓冲区的大小
      nextBatchSize.intValue
    } else
      maxLogMessageSize

    growBuffers(maxSize)
  }

  private def shouldDiscardBatch(batch: RecordBatch,
                                 transactionMetadata: CleanedTransactionMetadata,
                                 retainTxnMarkers: Boolean): Boolean = {
    if (batch.isControlBatch) {
      val canDiscardControlBatch = transactionMetadata.onControlBatchRead(batch)
      canDiscardControlBatch && !retainTxnMarkers
    } else {
      val canDiscardBatch = transactionMetadata.onBatchRead(batch)
      canDiscardBatch
    }
  }

  private def shouldRetainRecord(map: kafka.log.OffsetMap,
                                 retainDeletes: Boolean,
                                 batch: RecordBatch,
                                 record: Record,
                                 stats: CleanerStats): Boolean = {
    val pastLatestOffset = record.offset > map.latestOffset
    if (pastLatestOffset)
      return true

    if (record.hasKey) {
      val key = record.key
      val foundOffset = map.get(key)
      /* two cases in which we can get rid of a message:
       *   1) if there exists a message with the same key but higher offset
       *   2) if the message is a delete "tombstone" marker and enough time has passed
       */
      val redundant = foundOffset >= 0 && record.offset < foundOffset
      val obsoleteDelete = !retainDeletes && !record.hasValue
      !redundant && !obsoleteDelete
    } else {
      stats.invalidMessage()
      false
    }
  }

  /**
   * Double the I/O buffer capacity
   * 成倍增大IO缓冲区
   */
  def growBuffers(maxLogMessageSize: Int) {
    val maxBufferSize = math.max(maxLogMessageSize, maxIoBufferSize)
    if(readBuffer.capacity >= maxBufferSize || writeBuffer.capacity >= maxBufferSize)
      throw new IllegalStateException("This log contains a message larger than maximum allowable size of %s.".format(maxBufferSize))
    val newSize = math.min(this.readBuffer.capacity * 2, maxBufferSize)
    info("Growing cleaner I/O buffers from " + readBuffer.capacity + "bytes to " + newSize + " bytes.")
    this.readBuffer = ByteBuffer.allocate(newSize)
    this.writeBuffer = ByteBuffer.allocate(newSize)
  }

  /**
   * Restore the I/O buffer capacity to its original size
   * 将IO缓冲区大小变回原来的大小
   */
  def restoreBuffers() {
    if(this.readBuffer.capacity > this.ioBufferSize)
      this.readBuffer = ByteBuffer.allocate(this.ioBufferSize)
    if(this.writeBuffer.capacity > this.ioBufferSize)
      this.writeBuffer = ByteBuffer.allocate(this.ioBufferSize)
  }

  /**
   * Group the segments in a log into groups totaling less than a given size. the size is enforced separately for the log data and the index data.
   * We collect a group of such segments together into a single
   * destination segment. This prevents segment sizes from shrinking too much.
   *
   * @param segments The log segments to group
   * @param maxSize the maximum size in bytes for the total of all log data in a group
   * @param maxIndexSize the maximum size in bytes for the total of all index data in a group
   *
   * @return A list of grouped segments
   *
   * 将日志的日志段分组, 分组的总大小小于指定值. 日志数据和索引数据的大小分开独立计算.
   * 这里将一组日志段变成一个目标日志段, 这样可以防止日志段的大小缩小太快.
   *
   * 参数 segments: 需要分组的日志段
   * 参数 maxSize: 分组所有日志数据总和的最大大小
   * 参数 maxIndexSize:分组所有索引数据总和的最大大小
   */
  private[log] def groupSegmentsBySize(segments: Iterable[LogSegment], maxSize: Int, maxIndexSize: Int, firstUncleanableOffset: Long): List[Seq[LogSegment]] = {
    var grouped = List[List[LogSegment]]()
    var segs = segments.toList
    while(segs.nonEmpty) {
      var group = List(segs.head)
      var logSize = segs.head.size.toLong
      var indexSize = segs.head.offsetIndex.sizeInBytes.toLong
      var timeIndexSize = segs.head.timeIndex.sizeInBytes.toLong
      segs = segs.tail //去除第一个元素

      //如果还有日志段, 而且当前分组增加一个日志段后日志及索引大小都比阈值小, 而且日志段内的位移没有溢出,
      //则把该日志段添加到当前分组
      while(segs.nonEmpty &&
            logSize + segs.head.size <= maxSize &&
            indexSize + segs.head.offsetIndex.sizeInBytes <= maxIndexSize &&
            timeIndexSize + segs.head.timeIndex.sizeInBytes <= maxIndexSize &&
            lastOffsetForFirstSegment(segs, firstUncleanableOffset) - group.last.baseOffset <= Int.MaxValue) {
        group = segs.head :: group
        logSize += segs.head.size
        indexSize += segs.head.offsetIndex.sizeInBytes
        timeIndexSize += segs.head.timeIndex.sizeInBytes
        segs = segs.tail
      }
      grouped ::= group.reverse
    }
    grouped.reverse
  }

  /**
    * We want to get the last offset in the first log segment in segs.
    * LogSegment.nextOffset() gives the exact last offset in a segment, but can be expensive since it requires
    * scanning the segment from the last index entry.
    * Therefore, we estimate the last offset of the first log segment by using
    * the base offset of the next segment in the list.
    * If the next segment doesn't exist, first Uncleanable Offset will be used.
    *
    * @param segs - remaining segments to group.
    * @return The estimated last offset for the first segment in segs
    *
    * 这里获取segs中第一个日志段的最后位移
    * LogSegment.nextOffset()可以准确得到日志段的最后位移, 但是性能较差因为需要从最后的索引条目开始扫描日志段.
    * 因此这里使用下一个日志段的基准位移来计算前一个日志段的最后位移. 如果没有下一个日志段, 那么使用不可清理的初始位移.
    */
  private def lastOffsetForFirstSegment(segs: List[LogSegment], firstUncleanableOffset: Long): Long = {
    if (segs.size > 1) {
      /* if there is a next segment, use its base offset as the bounding offset to guarantee we know
       * the worst case offset */
      segs(1).baseOffset - 1
    } else {
      //for the last segment in the list, use the first uncleanable offset.
      firstUncleanableOffset - 1
    }
  }

  /**
   * Build a map of key_hash => offset for the keys in the cleanable dirty portion of the log to use in cleaning.
   * @param log The log to use
   * @param start The offset at which dirty messages begin
   * @param end The ending offset for the map that is being built
   * @param map The map in which to store the mappings
   * @param stats Collector for cleaning statistics
   *
   * 对于可清理脏日志的消息key建立key_hash => offset的映射, 此映射用于在清理过程
   */
  private[log] def buildOffsetMap(log: Log,
                                  start: Long,
                                  end: Long,
                                  map: OffsetMap,
                                  stats: CleanerStats) {
    map.clear()
    val dirty = log.logSegments(start, end).toBuffer
    info("Building offset map for log %s for %d segments in offset range [%d, %d).".format(log.name, dirty.size, start, end))

    //获取从start(包含)到end(不包含)的已回滚事务
    val abortedTransactions = log.collectAbortedTransactions(start, end)
    val transactionMetadata = CleanedTransactionMetadata(abortedTransactions)

    // Add all the cleanable dirty segments. We must take at least map.slots * load_factor,
    // but we may be able to fit more (if there is lots of duplication in the dirty section of the log)
    // 循环处理所有可清理的脏日志段, 直至映射的大小到达了(map.slots * load_factor)阈值,
    // 但是可能处理更多(如果脏日志段有很多的key重复的话)
    var full = false
    for (segment <- dirty if !full) {
      checkDone(log.topicPartition)

      full = buildOffsetMapForSegment(log.topicPartition, segment, map, start, log.config.maxMessageSize,
        transactionMetadata, stats)
      if (full)
        debug("Offset map is full, %d segments fully mapped, segment with base offset %d is partially mapped".format(dirty.indexOf(segment), segment.baseOffset))
    }
    info("Offset map for log %s complete.".format(log.name))
  }

  /**
   * Add the messages in the given segment to the offset map
   *
   * @param segment The segment to index
   * @param map The map in which to store the key=>offset mapping
   * @param stats Collector for cleaning statistics
   *
   * @return If the map was filled whilst loading from this segment
   *
   * 将指定日志段的消息添加到位移map中
   *
   * 参数 topicPartition: 主题和分区信息
   * 参数 segment: 需要索引的日志段
   * 参数 map: 存储key=>offset的map
   * 参数 startOffset: 起始位移
   * 参数 maxLogMessageSize: 日志中最大的消息大小
   * 参数 transactionMetadata: 用来跟踪清理日志过程中的事务状态的帮助类
   * 参数 stats: 清理指标的收集器
   *
   * 返回 在加载此日志段过程中map是否被填满
   */
  private def buildOffsetMapForSegment(topicPartition: TopicPartition,
                                       segment: LogSegment,
                                       map: OffsetMap,
                                       startOffset: Long,
                                       maxLogMessageSize: Int,
                                       transactionMetadata: CleanedTransactionMetadata,
                                       stats: CleanerStats): Boolean = {
    var position = segment.offsetIndex.lookup(startOffset).position
    val maxDesiredMapSize = (map.slots * this.dupBufferLoadFactor).toInt
    while (position < segment.log.sizeInBytes) {
      checkDone(topicPartition)
      readBuffer.clear()
      try {
        //从日志段的文件中读取消息数据
        segment.log.readInto(readBuffer, position)
      } catch {
        case e: Exception =>
          throw new KafkaException(s"Failed to read from segment $segment of partition $topicPartition " +
            "while loading offset map", e)
      }
      val records = MemoryRecords.readableRecords(readBuffer)
      //读取的字节数需要经过节流阀限制IO速率
      throttler.maybeThrottle(records.sizeInBytes)

      val startPosition = position
      for (batch <- records.batches.asScala) {
        if (batch.isControlBatch) {
          //处理控制类消息(commit/abort)
          transactionMetadata.onControlBatchRead(batch)
          stats.indexMessagesRead(1)
        } else {
          //该batch是否被回滚
          val isAborted = transactionMetadata.onBatchRead(batch)
          if (isAborted) {
            // If the batch is aborted, do not bother populating the offset map.
            // Note that abort markers are supported in v2 and above, which means count is defined.
            // 如果batch被回滚, 那么不用记录到offset map中
            // 另外回滚标记在v2或更高版本中支持, 同时batch中也会带有count统计字段
            stats.indexMessagesRead(batch.countOrNull)
          } else {
            //如果没有回滚则记录key => offset的映射
            for (record <- batch.asScala) {
              if (record.hasKey && record.offset >= startOffset) {
                if (map.size < maxDesiredMapSize)
                  map.put(record.key, record.offset)
                else
                  return true
              }
              stats.indexMessagesRead(1)
            }
          }
        }

        if (batch.lastOffset >= startOffset)
          map.updateLatestOffset(batch.lastOffset)
      }
      val bytesRead = records.validBytes
      position += bytesRead
      stats.indexBytesRead(bytesRead)

      // if we didn't read even one complete message, our read buffer may be too small
      // 如果没有读到一条完整的消息, 可能缓冲区过于小了, 增大缓冲区
      if(position == startPosition)
        growBuffersOrFail(segment.log, position, maxLogMessageSize, records)
    }
    restoreBuffers()
    false
  }
}

/**
 * A simple struct for collecting stats about log cleaning
 */
private class CleanerStats(time: Time = Time.SYSTEM) {
  val startTime = time.milliseconds
  var mapCompleteTime = -1L
  var endTime = -1L
  var bytesRead = 0L
  var bytesWritten = 0L
  var mapBytesRead = 0L
  var mapMessagesRead = 0L
  var messagesRead = 0L
  var invalidMessagesRead = 0L
  var messagesWritten = 0L
  var bufferUtilization = 0.0d

  def readMessages(messagesRead: Int, bytesRead: Int) {
    this.messagesRead += messagesRead
    this.bytesRead += bytesRead
  }

  def invalidMessage() {
    invalidMessagesRead += 1
  }

  def recopyMessages(messagesWritten: Int, bytesWritten: Int) {
    this.messagesWritten += messagesWritten
    this.bytesWritten += bytesWritten
  }

  def indexMessagesRead(size: Int) {
    mapMessagesRead += size
  }

  def indexBytesRead(size: Int) {
    mapBytesRead += size
  }

  def indexDone() {
    mapCompleteTime = time.milliseconds
  }

  def allDone() {
    endTime = time.milliseconds
  }

  def elapsedSecs = (endTime - startTime)/1000.0

  def elapsedIndexSecs = (mapCompleteTime - startTime)/1000.0

}

/**
 * Helper class for a log, its topic/partition, the first cleanable position, and the first uncleanable dirty position
 * 一个helper类, 包含日志, 主题/分区, 可清理的初始位置和不可清理的脏日志初始位置
 */
private case class LogToClean(topicPartition: TopicPartition, log: Log, firstDirtyOffset: Long, uncleanableOffset: Long) extends Ordered[LogToClean] {
  //已经清理的日志段的总字节数
  val cleanBytes = log.logSegments(-1, firstDirtyOffset).map(_.size.toLong).sum

  //第一个不可清理的日志段
  private[this] val firstUncleanableSegment = log.logSegments(uncleanableOffset, log.activeSegment.baseOffset).headOption.getOrElse(log.activeSegment)

  //不可清理日志段的初始位移
  val firstUncleanableOffset = firstUncleanableSegment.baseOffset

  //可清理的字节数
  val cleanableBytes = log.logSegments(firstDirtyOffset, math.max(firstDirtyOffset, firstUncleanableOffset)).map(_.size.toLong).sum

  // 总的字节数 = 已经清理的字节数 + 可清理的字节数
  val totalBytes = cleanBytes + cleanableBytes

  //可清理的比例
  val cleanableRatio = cleanableBytes / totalBytes.toDouble

  override def compare(that: LogToClean): Int = math.signum(this.cleanableRatio - that.cleanableRatio).toInt
}

private[log] object CleanedTransactionMetadata {
  def apply(abortedTransactions: List[AbortedTxn],
            transactionIndex: Option[TransactionIndex] = None): CleanedTransactionMetadata = {
    val queue = mutable.PriorityQueue.empty[AbortedTxn](new Ordering[AbortedTxn] {
      override def compare(x: AbortedTxn, y: AbortedTxn): Int = x.firstOffset compare y.firstOffset
    }.reverse)
    queue ++= abortedTransactions
    new CleanedTransactionMetadata(queue, transactionIndex)
  }

  val Empty = CleanedTransactionMetadata(List.empty[AbortedTxn])
}

/**
 * This is a helper class to facilitate tracking transaction state while cleaning the log. It is initialized
 * with the aborted transactions from the transaction index and its state is updated as the cleaner iterates through
 * the log during a round of cleaning. This class is responsible for deciding when transaction markers can
 * be removed and is therefore also responsible for updating the cleaned transaction index accordingly.
 *
 * 这是一个帮助类, 用来跟踪清理日志过程中的事务状态. 此类使用事务索引中的已回滚事务来初始化, 并且在cleaner清理过程中会更新
 * 状态. 此类负责决定事务标记是否可以被移除, 因此也负责更新已清理日志的事务索引.
 */
private[log] class CleanedTransactionMetadata(val abortedTransactions: mutable.PriorityQueue[AbortedTxn],
                                              val transactionIndex: Option[TransactionIndex] = None) {
  val ongoingCommittedTxns = mutable.Set.empty[Long]
  val ongoingAbortedTxns = mutable.Map.empty[Long, AbortedTransactionMetadata]

  /**
   * Update the cleaned transaction state with a control batch that has just been traversed by the cleaner.
   * Return true if the control batch can be discarded.
   *
   * 使用参数中的控制Batch更新已清理的事务状态. 如果控制batch被丢弃则返回true
   */
  def onControlBatchRead(controlBatch: RecordBatch): Boolean = {
    consumeAbortedTxnsUpTo(controlBatch.lastOffset)

    val controlRecord = controlBatch.iterator.next()
    val controlType = ControlRecordType.parse(controlRecord.key)
    val producerId = controlBatch.producerId
    controlType match {
      //如果控制消息为回滚消息
      case ControlRecordType.ABORT =>
        ongoingAbortedTxns.remove(producerId) match {
          // Retain the marker until all batches from the transaction have been removed
          // 保留标记直到同一事务中的所有batch都已经被删除
          case Some(abortedTxnMetadata) if abortedTxnMetadata.lastObservedBatchOffset.isDefined =>
            transactionIndex.foreach(_.append(abortedTxnMetadata.abortedTxn))
            false
          case _ => true
        }

      case ControlRecordType.COMMIT =>
        // This marker is eligible for deletion if we didn't traverse any batches from the transaction
        // 此标记可用于删除, 如果没有遇到同一事务中的任何batch的话
        !ongoingCommittedTxns.remove(producerId)

      case _ => false
    }
  }

  private def consumeAbortedTxnsUpTo(offset: Long): Unit = {
    while (abortedTransactions.headOption.exists(_.firstOffset <= offset)) {
      val abortedTxn = abortedTransactions.dequeue()
      ongoingAbortedTxns += abortedTxn.producerId -> new AbortedTransactionMetadata(abortedTxn)
    }
  }

  /**
   * Update the transactional state for the incoming non-control batch. If the batch is part of
   * an aborted transaction, return true to indicate that it is safe to discard.
   *
   * 对于参数中的非控制类batch更新事务状态. 如果该batch属于一个已回滚的事务, 那么返回true以指明可以安全丢弃
   */
  def onBatchRead(batch: RecordBatch): Boolean = {
    consumeAbortedTxnsUpTo(batch.lastOffset)
    if (batch.isTransactional) {
      ongoingAbortedTxns.get(batch.producerId) match {
        case Some(abortedTransactionMetadata) =>
          abortedTransactionMetadata.lastObservedBatchOffset = Some(batch.lastOffset)
          true
        case None =>
          ongoingCommittedTxns += batch.producerId
          false
      }
    } else {
      false
    }
  }

}

private class AbortedTransactionMetadata(val abortedTxn: AbortedTxn) {
  var lastObservedBatchOffset: Option[Long] = None
}

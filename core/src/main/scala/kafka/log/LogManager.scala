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

import java.io._
import java.nio.file.Files
import java.util.concurrent._

import com.yammer.metrics.core.Gauge
import kafka.metrics.KafkaMetricsGroup
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.server.{BrokerState, RecoveringFromUncleanShutdown, _}
import kafka.utils._
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.errors.{KafkaStorageException, LogDirNotFoundException}

import scala.collection.JavaConverters._
import scala.collection._
import scala.collection.mutable.ArrayBuffer

/**
 * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
 * All read and write operations are delegated to the individual log instances.
 *
 * The log manager maintains logs in one or more directories. New logs are created in the data directory
 * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
 * size or I/O rate.
 *
 * A background thread handles log retention by periodically truncating excess log segments.
 *
 * Kafka日志管理子系统的入口. 此日志管理器负责日志创建, 获取, 清理. 所有的读和写操作都会分派给单独的日志实例.
 *
 * 此日志管理器在一个或多个目录中维护日志. 新日志会在包含最少日志的目录中创建, 但不会基于大小或IO速率来进行负载均衡或移动分区.
 *
 * 一个后台线程会通过定期截断超出的日志来保留日志.
 */
@threadsafe
class LogManager(logDirs: Seq[File],
                 initialOfflineDirs: Seq[File],
                 val topicConfigs: Map[String, LogConfig], // note that this doesn't get updated after creation
                 val initialDefaultConfig: LogConfig,
                 val cleanerConfig: CleanerConfig,
                 recoveryThreadsPerDataDir: Int,
                 val flushCheckMs: Long,
                 val flushRecoveryOffsetCheckpointMs: Long,
                 val flushStartOffsetCheckpointMs: Long,
                 val retentionCheckMs: Long,
                 val maxPidExpirationMs: Int,
                 scheduler: Scheduler,
                 val brokerState: BrokerState,
                 brokerTopicStats: BrokerTopicStats,
                 logDirFailureChannel: LogDirFailureChannel,
                 time: Time) extends Logging with KafkaMetricsGroup {

  import LogManager._

  val LockFile = ".lock"
  val InitialTaskDelayMs = 30 * 1000

  private val logCreationOrDeletionLock = new Object
  private val currentLogs = new Pool[TopicPartition, Log]()
  // Future logs are put in the directory with "-future" suffix. Future log is created when user wants to move replica
  // from one log directory to another log directory on the same broker. The directory of the future log will be renamed
  // to replace the current log of the partition after the future log catches up with the current log
  // Future日志会放置在以"-future"结尾的目录中. 当使用者想将broker上的副本从一个目录移动到另一个目录中时, 就会创建future日志.
  // 当future日志追上当前的日志时, future日志的目录会重命名替代当前的日志
  private val futureLogs = new Pool[TopicPartition, Log]()

  // Each element in the queue contains the log object to be deleted and the time it is scheduled for deletion.
  // 此队列中的每个元素包含需要被删除的日志对象以及删除的时间
  private val logsToBeDeleted = new LinkedBlockingQueue[(Log, Long)]()

  private val _liveLogDirs: ConcurrentLinkedQueue[File] = createAndValidateLogDirs(logDirs, initialOfflineDirs)
  @volatile private var _currentDefaultConfig = initialDefaultConfig
  @volatile private var numRecoveryThreadsPerDataDir = recoveryThreadsPerDataDir

  def reconfigureDefaultLogConfig(logConfig: LogConfig): Unit = {
    this._currentDefaultConfig = logConfig
  }

  def currentDefaultConfig: LogConfig = _currentDefaultConfig

  def liveLogDirs: Seq[File] = {
    if (_liveLogDirs.size == logDirs.size)
      logDirs
    else
      _liveLogDirs.asScala.toBuffer
  }

  private val dirLocks = lockLogDirs(liveLogDirs)
  @volatile private var recoveryPointCheckpoints = liveLogDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, RecoveryPointCheckpointFile), logDirFailureChannel))).toMap
  @volatile private var logStartOffsetCheckpoints = liveLogDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, LogStartOffsetCheckpointFile), logDirFailureChannel))).toMap

  private val preferredLogDirs = new ConcurrentHashMap[TopicPartition, String]()

  private def offlineLogDirs: Iterable[File] = {
    val logDirsSet = mutable.Set[File](logDirs: _*)
    _liveLogDirs.asScala.foreach(logDirsSet -=)
    logDirsSet
  }

  //加载日志
  loadLogs()

  // public, so we can access this from kafka.admin.DeleteTopicTest
  // (compact)日志cleaner
  val cleaner: LogCleaner =
    if(cleanerConfig.enableCleaner)
      new LogCleaner(cleanerConfig, liveLogDirs, currentLogs, logDirFailureChannel, time = time)
    else
      null

  val offlineLogDirectoryCount = newGauge(
    "OfflineLogDirectoryCount",
    new Gauge[Int] {
      def value = offlineLogDirs.size
    }
  )

  for (dir <- logDirs) {
    newGauge(
      "LogDirectoryOffline",
      new Gauge[Int] {
        def value = if (_liveLogDirs.contains(dir)) 0 else 1
      },
      Map("logDirectory" -> dir.getAbsolutePath)
    )
  }

  /**
   * Create and check validity of the given directories that are not in the given offline directories, specifically:
   * <ol>
   * <li> Ensure that there are no duplicates in the directory list
   * <li> Create each directory if it doesn't exist
   * <li> Check that each path is a readable directory
   * </ol>
   * 创建并校验指定目录(非下线目录)的合法性, 具体来讲就是:
   *   1. 保证目录列表没有重复项;
   *   2. 如果目录不存在则创建它;
   *   3. 检查每个路径都是一个可读的目录
   */
  private def createAndValidateLogDirs(dirs: Seq[File], initialOfflineDirs: Seq[File]): ConcurrentLinkedQueue[File] = {
    val liveLogDirs = new ConcurrentLinkedQueue[File]()
    val canonicalPaths = mutable.HashSet.empty[String]

    for (dir <- dirs) {
      try {
        if (initialOfflineDirs.contains(dir))
          throw new IOException(s"Failed to load ${dir.getAbsolutePath} during broker startup")

        if (!dir.exists) {
          info(s"Log directory ${dir.getAbsolutePath} not found, creating it.")
          val created = dir.mkdirs()
          if (!created)
            throw new IOException(s"Failed to create data directory ${dir.getAbsolutePath}")
        }
        if (!dir.isDirectory || !dir.canRead)
          throw new IOException(s"${dir.getAbsolutePath} is not a readable log directory.")

        // getCanonicalPath() throws IOException if a file system query fails or if the path is invalid (e.g. contains
        // the Nul character). Since there's no easy way to distinguish between the two cases, we treat them the same
        // and mark the log directory as offline.
        if (!canonicalPaths.add(dir.getCanonicalPath))
          throw new KafkaException(s"Duplicate log directory found: ${dirs.mkString(", ")}")


        liveLogDirs.add(dir)
      } catch {
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Failed to create or validate data directory ${dir.getAbsolutePath}", e)
      }
    }
    if (liveLogDirs.isEmpty) {
      fatal(s"Shutdown broker because none of the specified log dirs from ${dirs.mkString(", ")} can be created or validated")
      Exit.halt(1)
    }

    liveLogDirs
  }

  def resizeRecoveryThreadPool(newSize: Int): Unit = {
    info(s"Resizing recovery thread pool size for each data dir from $numRecoveryThreadsPerDataDir to $newSize")
    numRecoveryThreadsPerDataDir = newSize
  }

  // dir should be an absolute path
  // 处理日志目录失败的情况, dir应该为绝对路径
  def handleLogDirFailure(dir: String) {
    info(s"Stopping serving logs in dir $dir")
    logCreationOrDeletionLock synchronized {
      _liveLogDirs.remove(new File(dir))
      if (_liveLogDirs.isEmpty) {
        fatal(s"Shutdown broker because all log dirs in ${logDirs.mkString(", ")} have failed")
        Exit.halt(1)
      }

      recoveryPointCheckpoints = recoveryPointCheckpoints.filter { case (file, _) => file.getAbsolutePath != dir }
      logStartOffsetCheckpoints = logStartOffsetCheckpoints.filter { case (file, _) => file.getAbsolutePath != dir }

      //通知cleaner停止处理该日志目录
      if (cleaner != null)
        cleaner.handleLogDirFailure(dir)

      //如果该日志目录为当前使用的日志目录, 把该目录加到当前的offline集合中
      val offlineCurrentTopicPartitions = currentLogs.collect {
        case (tp, log) if log.dir.getParent == dir => tp
      }
      offlineCurrentTopicPartitions.foreach { topicPartition => {
        val removedLog = currentLogs.remove(topicPartition)
        if (removedLog != null) {
          removedLog.closeHandlers()
          removedLog.removeLogMetrics()
        }
      }}

      //如果该日志目录为future的日志目录, 把该目录加到future的offline集合中
      val offlineFutureTopicPartitions = futureLogs.collect {
        case (tp, log) if log.dir.getParent == dir => tp
      }
      offlineFutureTopicPartitions.foreach { topicPartition => {
        val removedLog = futureLogs.remove(topicPartition)
        if (removedLog != null) {
          removedLog.closeHandlers()
          removedLog.removeLogMetrics()
        }
      }}

      info(s"Logs for partitions ${offlineCurrentTopicPartitions.mkString(",")} are offline and " +
           s"logs for future partitions ${offlineFutureTopicPartitions.mkString(",")} are offline due to failure on log directory $dir")
      dirLocks.filter(_.file.getParent == dir).foreach(dir => CoreUtils.swallow(dir.destroy(), this))
    }
  }

  /**
   * Lock all the given directories
   * 对指定的目录列表全部进行加锁(文件锁)
   */
  private def lockLogDirs(dirs: Seq[File]): Seq[FileLock] = {
    dirs.flatMap { dir =>
      try {
        val lock = new FileLock(new File(dir, LockFile))
        if (!lock.tryLock())
          throw new KafkaException("Failed to acquire lock on file .lock in " + lock.file.getParent +
            ". A Kafka instance in another process or thread is using this directory.")
        Some(lock)
      } catch {
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Disk error while locking directory $dir", e)
          None
      }
    }
  }

  private def addLogToBeDeleted(log: Log): Unit = {
    this.logsToBeDeleted.add((log, time.milliseconds()))
  }

  // Only for testing
  private[log] def hasLogsToBeDeleted: Boolean = !logsToBeDeleted.isEmpty

  private def loadLog(logDir: File, recoveryPoints: Map[TopicPartition, Long], logStartOffsets: Map[TopicPartition, Long]): Unit = {
    debug("Loading log '" + logDir.getName + "'")
    //日志分区
    val topicPartition = Log.parseTopicPartitionName(logDir)

    //分区配置
    val config = topicConfigs.getOrElse(topicPartition.topic, currentDefaultConfig)

    //恢复点
    val logRecoveryPoint = recoveryPoints.getOrElse(topicPartition, 0L)

    //日志起始点
    val logStartOffset = logStartOffsets.getOrElse(topicPartition, 0L)

    //加载日志
    val log = Log(
      dir = logDir,
      config = config,
      logStartOffset = logStartOffset,
      recoveryPoint = logRecoveryPoint,
      maxProducerIdExpirationMs = maxPidExpirationMs,
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
      scheduler = scheduler,
      time = time,
      brokerTopicStats = brokerTopicStats,
      logDirFailureChannel = logDirFailureChannel)

    if (logDir.getName.endsWith(Log.DeleteDirSuffix)) {
      //以-delete结尾的日志为需要删除的日志
      addLogToBeDeleted(log)
    } else {
      val previous = {
        if (log.isFuture)
          this.futureLogs.put(topicPartition, log)
        else
          this.currentLogs.put(topicPartition, log)
      }
      if (previous != null) {
        if (log.isFuture)
          throw new IllegalStateException("Duplicate log directories found: %s, %s!".format(log.dir.getAbsolutePath, previous.dir.getAbsolutePath))
        else
          throw new IllegalStateException(s"Duplicate log directories for $topicPartition are found in both ${log.dir.getAbsolutePath} " +
            s"and ${previous.dir.getAbsolutePath}. It is likely because log directory failure happened while broker was " +
            s"replacing current replica with future replica. Recover broker from this failure by manually deleting one of the two directories " +
            s"for this partition. It is recommended to delete the partition in the log directory that is known to have failed recently.")
      }
    }
  }

  /**
   * Recover and load all logs in the given data directories
   * 恢复以及加载指定目录下的所有日志
   */
  private def loadLogs(): Unit = {
    info("Loading logs.")
    val startMs = time.milliseconds
    val threadPools = ArrayBuffer.empty[ExecutorService]
    val offlineDirs = mutable.Set.empty[(String, IOException)]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    for (dir <- liveLogDirs) {
      try {
        //每个数据目录都有单独的线程池来恢复
        val pool = Executors.newFixedThreadPool(numRecoveryThreadsPerDataDir)
        threadPools.append(pool)

        val cleanShutdownFile = new File(dir, Log.CleanShutdownFile)

        //如果没有.kafka_cleanshutdown文件, 那么上一次退出为非优雅退出.
        if (cleanShutdownFile.exists) {
          debug(s"Found clean shutdown file. Skipping recovery for all logs in data directory: ${dir.getAbsolutePath}")
        } else {
          // log recovery itself is being performed by `Log` class during initialization
          brokerState.newState(RecoveringFromUncleanShutdown)
        }

        var recoveryPoints = Map[TopicPartition, Long]()
        try {
          //从recovery-point-offset-checkpoint文件中读取日志恢复点
          recoveryPoints = this.recoveryPointCheckpoints(dir).read
        } catch {
          case e: Exception =>
            warn("Error occurred while reading recovery-point-offset-checkpoint file of directory " + dir, e)
            warn("Resetting the recovery checkpoint to 0")
        }

        var logStartOffsets = Map[TopicPartition, Long]()
        try {
          //从log-start-offset-checkpoint文件中读取日志起始位移
          logStartOffsets = this.logStartOffsetCheckpoints(dir).read
        } catch {
          case e: Exception =>
            warn("Error occurred while reading log-start-offset-checkpoint file of directory " + dir, e)
        }

        val jobsForDir = for {
          dirContent <- Option(dir.listFiles).toList
          logDir <- dirContent if logDir.isDirectory
        } yield {
          CoreUtils.runnable {
            try {
              //列举数据目录下的日志, 并加载之
              loadLog(logDir, recoveryPoints, logStartOffsets)
            } catch {
              case e: IOException =>
                offlineDirs.add((dir.getAbsolutePath, e))
                error("Error while loading log dir " + dir.getAbsolutePath, e)
            }
          }
        }
        jobs(cleanShutdownFile) = jobsForDir.map(pool.submit)
      } catch {
        case e: IOException =>
          offlineDirs.add((dir.getAbsolutePath, e))
          error("Error while loading log dir " + dir.getAbsolutePath, e)
      }
    }

    try {
      for ((cleanShutdownFile, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)
        try {
          //加载完日之后删除之前的优雅退出标识文件
          cleanShutdownFile.delete()
        } catch {
          case e: IOException =>
            offlineDirs.add((cleanShutdownFile.getParent, e))
            error(s"Error while deleting the clean shutdown file $cleanShutdownFile", e)
        }
      }

      offlineDirs.foreach { case (dir, e) =>
        logDirFailureChannel.maybeAddOfflineLogDir(dir, s"Error while deleting the clean shutdown file in dir $dir", e)
      }
    } catch {
      case e: ExecutionException =>
        error("There was an error in one of the threads during logs loading: " + e.getCause)
        throw e.getCause
    } finally {
      threadPools.foreach(_.shutdown())
    }

    info(s"Logs loading complete in ${time.milliseconds - startMs} ms.")
  }

  /**
   *  Start the background threads to flush logs and do log cleanup
   *  启动日志刷盘和清理的后台线程
   */
  def startup() {
    /* Schedule the cleanup task to delete old logs */
    if (scheduler != null) {
      info("Starting log cleanup with a period of %d ms.".format(retentionCheckMs))
      //对于非compact的日志, 启动定期清理日志段的后台任务
      scheduler.schedule("kafka-log-retention",
                         cleanupLogs _,
                         delay = InitialTaskDelayMs,
                         period = retentionCheckMs,
                         TimeUnit.MILLISECONDS)

      //刷盘脏日志
      info("Starting log flusher with a default period of %d ms.".format(flushCheckMs))
      scheduler.schedule("kafka-log-flusher",
                         flushDirtyLogs _,
                         delay = InitialTaskDelayMs,
                         period = flushCheckMs,
                         TimeUnit.MILLISECONDS)

      //将恢复点做checkpoint
      scheduler.schedule("kafka-recovery-point-checkpoint",
                         checkpointLogRecoveryOffsets _,
                         delay = InitialTaskDelayMs,
                         period = flushRecoveryOffsetCheckpointMs,
                         TimeUnit.MILLISECONDS)

      //将logStartOffset做checkpoint
      scheduler.schedule("kafka-log-start-offset-checkpoint",
                         checkpointLogStartOffsets _,
                         delay = InitialTaskDelayMs,
                         period = flushStartOffsetCheckpointMs,
                         TimeUnit.MILLISECONDS)

      //删除被标记为删除的日志
      scheduler.schedule("kafka-delete-logs", // will be rescheduled after each delete logs with a dynamic period
                         deleteLogs _,
                         delay = InitialTaskDelayMs,
                         unit = TimeUnit.MILLISECONDS)
    }
    if (cleanerConfig.enableCleaner)
      cleaner.startup()
  }

  /**
   * Close all the logs
   * 关闭所有日志
   */
  def shutdown() {
    info("Shutting down.")

    removeMetric("OfflineLogDirectoryCount")
    for (dir <- logDirs) {
      removeMetric("LogDirectoryOffline", Map("logDirectory" -> dir.getAbsolutePath))
    }

    val threadPools = ArrayBuffer.empty[ExecutorService]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    // stop the cleaner first
    // 先停止compact日志的cleaner
    if (cleaner != null) {
      CoreUtils.swallow(cleaner.shutdown(), this)
    }

    // close logs in each dir
    for (dir <- liveLogDirs) {
      debug("Flushing and closing logs at " + dir)

      val pool = Executors.newFixedThreadPool(numRecoveryThreadsPerDataDir)
      threadPools.append(pool)

      val logsInDir = logsByDir.getOrElse(dir.toString, Map()).values

      val jobsForDir = logsInDir map { log =>
        CoreUtils.runnable {
          // flush the log to ensure latest possible recovery point
          log.flush()
          log.close()
        }
      }

      jobs(dir) = jobsForDir.map(pool.submit).toSeq
    }

    try {
      for ((dir, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)

        // update the last flush point
        // 更新最后的刷盘点
        debug("Updating recovery points at " + dir)
        checkpointLogRecoveryOffsetsInDir(dir)

        debug("Updating log start offsets at " + dir)
        checkpointLogStartOffsetsInDir(dir)

        // mark that the shutdown was clean by creating marker file
        debug("Writing clean shutdown marker at " + dir)
        CoreUtils.swallow(Files.createFile(new File(dir, Log.CleanShutdownFile).toPath), this)
      }
    } catch {
      case e: ExecutionException =>
        error("There was an error in one of the threads during LogManager shutdown: " + e.getCause)
        throw e.getCause
    } finally {
      threadPools.foreach(_.shutdown())
      // regardless of whether the close succeeded, we need to unlock the data directories
      dirLocks.foreach(_.destroy())
    }

    info("Shutdown complete.")
  }

  /**
   * Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
   *
   * @param partitionOffsets Partition logs that need to be truncated
   * @param isFuture True iff the truncation should be performed on the future log of the specified partitions
   *
   * 将分区日志截断至指定位移, 并且将恢复点更新为该位移
   */
  def truncateTo(partitionOffsets: Map[TopicPartition, Long], isFuture: Boolean) {
    var truncated = false
    for ((topicPartition, truncateOffset) <- partitionOffsets) {
      val log = {
        if (isFuture)
          futureLogs.get(topicPartition)
        else
          currentLogs.get(topicPartition)
      }
      // If the log does not exist, skip it
      if (log != null) {
        //May need to abort and pause the cleaning of the log, and resume after truncation is done.
        //终止并暂停日志的清理, 在截断完成后再恢复清理
        val needToStopCleaner = cleaner != null && truncateOffset < log.activeSegment.baseOffset
        if (needToStopCleaner && !isFuture)
          cleaner.abortAndPauseCleaning(topicPartition)
        try {
          if (log.truncateTo(truncateOffset))
            truncated = true
          if (needToStopCleaner && !isFuture)
            cleaner.maybeTruncateCheckpoint(log.dir.getParentFile, topicPartition, log.activeSegment.baseOffset)
        } finally {
          if (needToStopCleaner && !isFuture)
            cleaner.resumeCleaning(topicPartition)
        }
      }
    }

    if (truncated)
      checkpointLogRecoveryOffsets()
  }

  /**
   * Delete all data in a partition and start the log at the new offset
   *
   * @param topicPartition The partition whose log needs to be truncated
   * @param newOffset The new offset to start the log with
   * @param isFuture True iff the truncation should be performed on the future log of the specified partition
   *
   * 删除一个分区的所有数据并且从指定的新位移开始新的日志
   */
  def truncateFullyAndStartAt(topicPartition: TopicPartition, newOffset: Long, isFuture: Boolean) {
    val log = {
      if (isFuture)
        futureLogs.get(topicPartition)
      else
        currentLogs.get(topicPartition)
    }
    // If the log does not exist, skip it
    if (log != null) {
        //Abort and pause the cleaning of the log, and resume after truncation is done.
        //终止并暂停日志的清理, 在截断完成后再恢复清理
      if (cleaner != null && !isFuture)
        cleaner.abortAndPauseCleaning(topicPartition)
      log.truncateFullyAndStartAt(newOffset)
      if (cleaner != null && !isFuture) {
        cleaner.maybeTruncateCheckpoint(log.dir.getParentFile, topicPartition, log.activeSegment.baseOffset)
        cleaner.resumeCleaning(topicPartition)
      }
      checkpointLogRecoveryOffsetsInDir(log.dir.getParentFile)
    }
  }

  /**
   * Write out the current recovery point for all logs to a text file in the log directory
   * to avoid recovering the whole log on startup.
   * 将当前所有日志的恢复点持久化到一个文件避免启动时对整个日志进行恢复
   */
  def checkpointLogRecoveryOffsets() {
    liveLogDirs.foreach(checkpointLogRecoveryOffsetsInDir)
  }

  /**
   * Write out the current log start offset for all logs to a text file in the log directory
   * to avoid exposing data that have been deleted by DeleteRecordsRequest
   * 将当前所有日志的logStartOffset持久化到一个文件避免暴露被DeleteRecordsRequest删除的数据
   */
  def checkpointLogStartOffsets() {
    liveLogDirs.foreach(checkpointLogStartOffsetsInDir)
  }

  /**
   * Make a checkpoint for all logs in provided directory.
   * 对指定目录下的所有日志生成一个检查点
   */
  private def checkpointLogRecoveryOffsetsInDir(dir: File): Unit = {
    for {
      partitionToLog <- logsByDir.get(dir.getAbsolutePath)
      checkpoint <- recoveryPointCheckpoints.get(dir)
    } {
      try {
        checkpoint.write(partitionToLog.mapValues(_.recoveryPoint))
        allLogs.foreach(_.deleteSnapshotsAfterRecoveryPointCheckpoint())
      } catch {
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Disk error while writing to recovery point " +
            s"file in directory $dir", e)
      }
    }
  }

  /**
   * Checkpoint log start offset for all logs in provided directory.
   * 对指定目录的所有日志的logStartOffset进行checkpoint
   */
  private def checkpointLogStartOffsetsInDir(dir: File): Unit = {
    for {
      partitionToLog <- logsByDir.get(dir.getAbsolutePath)
      checkpoint <- logStartOffsetCheckpoints.get(dir)
    } {
      try {
        val logStartOffsets = partitionToLog.filter { case (_, log) =>
          log.logStartOffset > log.logSegments.head.baseOffset
        }.mapValues(_.logStartOffset)
        checkpoint.write(logStartOffsets)
      } catch {
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Disk error while writing to logStartOffset file in directory $dir", e)
      }
    }
  }

  // The logDir should be an absolute path
  def maybeUpdatePreferredLogDir(topicPartition: TopicPartition, logDir: String): Unit = {
    // Do not cache the preferred log directory if either the current log or the future log for this partition exists in the specified logDir
    if (!getLog(topicPartition).exists(_.dir.getParent == logDir) &&
        !getLog(topicPartition, isFuture = true).exists(_.dir.getParent == logDir))
      preferredLogDirs.put(topicPartition, logDir)
  }

  def abortAndPauseCleaning(topicPartition: TopicPartition): Unit = {
    if (cleaner != null)
      cleaner.abortAndPauseCleaning(topicPartition)
  }


  /**
   * Get the log if it exists, otherwise return None
   * 获取一个日志(如果它存在的话), 否则返回None
   *
   * @param topicPartition the partition of the log
   * @param isFuture True iff the future log of the specified partition should be returned
   */
  def getLog(topicPartition: TopicPartition, isFuture: Boolean = false): Option[Log] = {
    if (isFuture)
      Option(futureLogs.get(topicPartition))
    else
      Option(currentLogs.get(topicPartition))
  }

  /**
   * If the log already exists, just return a copy of the existing log
   * Otherwise if isNew=true or if there is no offline log directory, create a log for the given topic and the given partition
   * Otherwise throw KafkaStorageException
   *
   * @param topicPartition The partition whose log needs to be returned or created
   * @param config The configuration of the log that should be applied for log creation
   * @param isNew Whether the replica should have existed on the broker or not
   * @param isFuture True iff the future log of the specified partition should be returned or created
   * @throws KafkaStorageException if isNew=false, log is not found in the cache and there is offline log directory on the broker
   *
   * 如果日志已经存在, 那么返回该日志;
   * 否则, 如果isNew为true或者不是offline的日志目录, 对于指定主题和分区创建一个日志;
   * 否则, 抛出KafkaStorageException
   */
  def getOrCreateLog(topicPartition: TopicPartition, config: LogConfig, isNew: Boolean = false, isFuture: Boolean = false): Log = {
    logCreationOrDeletionLock synchronized {
      getLog(topicPartition, isFuture).getOrElse {
        // create the log if it has not already been created in another thread
        if (!isNew && offlineLogDirs.nonEmpty)
          throw new KafkaStorageException(s"Can not create log for $topicPartition because log directories ${offlineLogDirs.mkString(",")} are offline")

        val logDir = {
          val preferredLogDir = preferredLogDirs.get(topicPartition)

          if (isFuture) {
            if (preferredLogDir == null)
              throw new IllegalStateException(s"Can not create the future log for $topicPartition without having a preferred log directory")
            else if (getLog(topicPartition).get.dir.getParent == preferredLogDir)
              throw new IllegalStateException(s"Can not create the future log for $topicPartition in the current log directory of this partition")
          }

          if (preferredLogDir != null)
            preferredLogDir
          else
            nextLogDir().getAbsolutePath
        }
        if (!isLogDirOnline(logDir))
          throw new KafkaStorageException(s"Can not create log for $topicPartition because log directory $logDir is offline")

        try {
          val dir = {
            if (isFuture)
              new File(logDir, Log.logFutureDirName(topicPartition))
            else
              new File(logDir, Log.logDirName(topicPartition))
          }
          Files.createDirectories(dir.toPath)

          val log = Log(
            dir = dir,
            config = config,
            logStartOffset = 0L,
            recoveryPoint = 0L,
            maxProducerIdExpirationMs = maxPidExpirationMs,
            producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
            scheduler = scheduler,
            time = time,
            brokerTopicStats = brokerTopicStats,
            logDirFailureChannel = logDirFailureChannel)

          if (isFuture)
            futureLogs.put(topicPartition, log)
          else
            currentLogs.put(topicPartition, log)

          info(s"Created log for partition $topicPartition in $logDir with properties " +
            s"{${config.originals.asScala.mkString(", ")}}.")
          // Remove the preferred log dir since it has already been satisfied
          preferredLogDirs.remove(topicPartition)

          log
        } catch {
          case e: IOException =>
            val msg = s"Error while creating log for $topicPartition in dir $logDir"
            logDirFailureChannel.maybeAddOfflineLogDir(logDir, msg, e)
            throw new KafkaStorageException(msg, e)
        }
      }
    }
  }

  /**
   *  Delete logs marked for deletion. Delete all logs for which `currentDefaultConfig.fileDeleteDelayMs`
   *  has elapsed after the delete was scheduled. Logs for which this interval has not yet elapsed will be
   *  considered for deletion in the next iteration of `deleteLogs`. The next iteration will be executed
   *  after the remaining time for the first log that is not deleted. If there are no more `logsToBeDeleted`,
   *  `deleteLogs` will be executed after `currentDefaultConfig.fileDeleteDelayMs`.
   *  删除被标记为删除的日志.
   *  此方法删除所有被调度删除且经过了`currentDefaultConfig.fileDeleteDelayMs`时间的日志. 对于还没有超过该时间间隔的日志
   *  将会在下次`deleteLogs`迭代中检查是否被删除. 下一个迭代将会在第一个未删除日志的剩余时间之后执行. 如果没有需要删除的日志,
   *  那么`deleteLogs`会在`currentDefaultConfig.fileDeleteDelayMs`后执行.
   */
  private def deleteLogs(): Unit = {
    var nextDelayMs = 0L
    try {
      def nextDeleteDelayMs: Long = {
        if (!logsToBeDeleted.isEmpty) {
          val (_, scheduleTimeMs) = logsToBeDeleted.peek()
          scheduleTimeMs + currentDefaultConfig.fileDeleteDelayMs - time.milliseconds()
        } else
          currentDefaultConfig.fileDeleteDelayMs
      }

      while ({nextDelayMs = nextDeleteDelayMs; nextDelayMs <= 0}) {
        val (removedLog, _) = logsToBeDeleted.take()
        if (removedLog != null) {
          try {
            removedLog.delete()
            info(s"Deleted log for partition ${removedLog.topicPartition} in ${removedLog.dir.getAbsolutePath}.")
          } catch {
            case e: KafkaStorageException =>
              error(s"Exception while deleting $removedLog in dir ${removedLog.dir.getParent}.", e)
          }
        }
      }
    } catch {
      case e: Throwable =>
        error(s"Exception in kafka-delete-logs thread.", e)
    } finally {
      try {
        scheduler.schedule("kafka-delete-logs",
          deleteLogs _,
          delay = nextDelayMs,
          unit = TimeUnit.MILLISECONDS)
      } catch {
        case e: Throwable =>
          if (scheduler.isStarted) {
            // No errors should occur unless scheduler has been shutdown
            error(s"Failed to schedule next delete in kafka-delete-logs thread", e)
          }
      }
    }
  }

  /**
    * Mark the partition directory in the source log directory for deletion and
    * rename the future log of this partition in the destination log directory to be the current log
    *
    * @param topicPartition TopicPartition that needs to be swapped
    *
    * 将原日志目录下的分区目录标识为删除, 并且将此分区目标目录下的future日志重命名替代当前日志
    */
  def replaceCurrentWithFutureLog(topicPartition: TopicPartition): Unit = {
    logCreationOrDeletionLock synchronized {
      val sourceLog = currentLogs.get(topicPartition)
      val destLog = futureLogs.get(topicPartition)

      if (sourceLog == null)
        throw new KafkaStorageException(s"The current replica for $topicPartition is offline")
      if (destLog == null)
        throw new KafkaStorageException(s"The future replica for $topicPartition is offline")

      destLog.renameDir(Log.logDirName(topicPartition))
      // Now that future replica has been successfully renamed to be the current replica
      // Update the cached map and log cleaner as appropriate.
      // 到这里证明future的副本已经成功重命名为当前副本, 更新缓存和日志cleaner
      futureLogs.remove(topicPartition)
      currentLogs.put(topicPartition, destLog)
      if (cleaner != null) {
        cleaner.alterCheckpointDir(topicPartition, sourceLog.dir.getParentFile, destLog.dir.getParentFile)
        cleaner.resumeCleaning(topicPartition)
      }

      try {
        sourceLog.renameDir(Log.logDeleteDirName(topicPartition))
        // Now that replica in source log directory has been successfully renamed for deletion.
        // Close the log, update checkpoint files, and enqueue this log to be deleted.
        // 到这里证明原目录副本已经成功重命名为删除目录, 关闭日志, 更新检查点文件, 并将此日志入队等待被删除.
        sourceLog.close()
        checkpointLogRecoveryOffsetsInDir(sourceLog.dir.getParentFile)
        checkpointLogStartOffsetsInDir(sourceLog.dir.getParentFile)
        addLogToBeDeleted(sourceLog)
      } catch {
        case e: KafkaStorageException =>
          // If sourceLog's log directory is offline, we need close its handlers here.
          // handleLogDirFailure() will not close handlers of sourceLog because it has been removed from currentLogs map
          sourceLog.closeHandlers()
          sourceLog.removeLogMetrics()
          throw e
      }

      info(s"The current replica is successfully replaced with the future replica for $topicPartition")
    }
  }

  /**
    * Rename the directory of the given topic-partition "logdir" as "logdir.uuid.delete" and
    * add it in the queue for deletion.
    *
    * @param topicPartition TopicPartition that needs to be deleted
    * @param isFuture True iff the future log of the specified partition should be deleted
    * @return the removed log
    *
    * 将指定topic-partition的目录"logdir"重命名为"logdir.uuid.delete", 并且并将此日志入队等待被删除.
    */
  def asyncDelete(topicPartition: TopicPartition, isFuture: Boolean = false): Log = {
    val removedLog: Log = logCreationOrDeletionLock synchronized {
      if (isFuture)
        futureLogs.remove(topicPartition)
      else
        currentLogs.remove(topicPartition)
    }
    if (removedLog != null) {
      //We need to wait until there is no more cleaning task on the log to be deleted before actually deleting it.
      if (cleaner != null && !isFuture) {
        cleaner.abortCleaning(topicPartition)
        cleaner.updateCheckpoints(removedLog.dir.getParentFile)
      }
      removedLog.renameDir(Log.logDeleteDirName(topicPartition))
      checkpointLogRecoveryOffsetsInDir(removedLog.dir.getParentFile)
      checkpointLogStartOffsetsInDir(removedLog.dir.getParentFile)
      addLogToBeDeleted(removedLog)
      info(s"Log for partition ${removedLog.topicPartition} is renamed to ${removedLog.dir.getAbsolutePath} and is scheduled for deletion")
    } else if (offlineLogDirs.nonEmpty) {
      throw new KafkaStorageException("Failed to delete log for " + topicPartition + " because it may be in one of the offline directories " + offlineLogDirs.mkString(","))
    }
    removedLog
  }

  /**
   * Choose the next directory in which to create a log. Currently this is done
   * by calculating the number of partitions in each directory and then choosing the
   * data directory with the fewest partitions.
   */
  private def nextLogDir(): File = {
    if(_liveLogDirs.size == 1) {
      _liveLogDirs.peek()
    } else {
      // count the number of logs in each parent directory (including 0 for empty directories
      val logCounts = allLogs.groupBy(_.dir.getParent).mapValues(_.size)
      val zeros = _liveLogDirs.asScala.map(dir => (dir.getPath, 0)).toMap
      val dirCounts = (zeros ++ logCounts).toBuffer

      // choose the directory with the least logs in it
      val leastLoaded = dirCounts.sortBy(_._2).head
      new File(leastLoaded._1)
    }
  }

  /**
   * Delete any eligible logs. Return the number of segments deleted.
   * Only consider logs that are not compacted.
   * 删除任何可以被删除的日志, 并返回删除的日志段数量. 这里只需要考虑非compact的日志
   */
  def cleanupLogs() {
    debug("Beginning log cleanup...")
    var total = 0
    val startMs = time.milliseconds
    for(log <- allLogs; if !log.config.compact) {
      debug("Garbage collecting '" + log.name + "'")
      total += log.deleteOldSegments()
    }
    debug("Log cleanup completed. " + total + " files deleted in " +
                  (time.milliseconds - startMs) / 1000 + " seconds")
  }

  /**
   * Get all the partition logs
   */
  def allLogs: Iterable[Log] = currentLogs.values ++ futureLogs.values

  def logsByTopic(topic: String): Seq[Log] = {
    (currentLogs.toList ++ futureLogs.toList).filter { case (topicPartition, _) =>
      topicPartition.topic() == topic
    }.map { case (_, log) => log }
  }

  /**
   * Map of log dir to logs by topic and partitions in that dir
   */
  private def logsByDir: Map[String, Map[TopicPartition, Log]] = {
    (this.currentLogs.toList ++ this.futureLogs.toList).groupBy {
      case (_, log) => log.dir.getParent
    }.mapValues(_.toMap)
  }

  // logDir should be an absolute path
  def isLogDirOnline(logDir: String): Boolean = {
    // The logDir should be an absolute path
    if (!logDirs.exists(_.getAbsolutePath == logDir))
      throw new LogDirNotFoundException(s"Log dir $logDir is not found in the config.")

    _liveLogDirs.contains(new File(logDir))
  }

  /**
   * Flush any log which has exceeded its flush interval and has unwritten messages.
   * 将超过刷盘时间间隔且有未持久化消息的日志进行刷盘
   */
  private def flushDirtyLogs(): Unit = {
    debug("Checking for dirty logs to flush...")

    for ((topicPartition, log) <- currentLogs.toList ++ futureLogs.toList) {
      try {
        val timeSinceLastFlush = time.milliseconds - log.lastFlushTime
        debug("Checking if flush is needed on " + topicPartition.topic + " flush interval  " + log.config.flushMs +
              " last flushed " + log.lastFlushTime + " time since last flush: " + timeSinceLastFlush)
        if(timeSinceLastFlush >= log.config.flushMs)
          log.flush
      } catch {
        case e: Throwable =>
          error("Error flushing topic " + topicPartition.topic, e)
      }
    }
  }
}

object LogManager {

  val RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint"
  val LogStartOffsetCheckpointFile = "log-start-offset-checkpoint"
  val ProducerIdExpirationCheckIntervalMs = 10 * 60 * 1000

  def apply(config: KafkaConfig,
            initialOfflineDirs: Seq[String],
            zkClient: KafkaZkClient,
            brokerState: BrokerState,
            kafkaScheduler: KafkaScheduler,
            time: Time,
            brokerTopicStats: BrokerTopicStats,
            logDirFailureChannel: LogDirFailureChannel): LogManager = {
    val defaultProps = KafkaServer.copyKafkaConfigToLog(config)
    val defaultLogConfig = LogConfig(defaultProps)

    // read the log configurations from zookeeper
    val (topicConfigs, failed) = zkClient.getLogConfigs(zkClient.getAllTopicsInCluster, defaultProps)
    if (!failed.isEmpty) throw failed.head._2

    val cleanerConfig = LogCleaner.cleanerConfig(config)

    new LogManager(logDirs = config.logDirs.map(new File(_).getAbsoluteFile),
      initialOfflineDirs = initialOfflineDirs.map(new File(_).getAbsoluteFile),
      topicConfigs = topicConfigs,
      initialDefaultConfig = defaultLogConfig,
      cleanerConfig = cleanerConfig,
      recoveryThreadsPerDataDir = config.numRecoveryThreadsPerDataDir,
      flushCheckMs = config.logFlushSchedulerIntervalMs,
      flushRecoveryOffsetCheckpointMs = config.logFlushOffsetCheckpointIntervalMs,
      flushStartOffsetCheckpointMs = config.logFlushStartOffsetCheckpointIntervalMs,
      retentionCheckMs = config.logCleanupIntervalMs,
      maxPidExpirationMs = config.transactionIdExpirationMs,
      scheduler = kafkaScheduler,
      brokerState = brokerState,
      brokerTopicStats = brokerTopicStats,
      logDirFailureChannel = logDirFailureChannel,
      time = time)
  }
}

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

import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import com.yammer.metrics.core.Gauge
import kafka.common.LogCleaningAbortedException
import kafka.metrics.KafkaMetricsGroup
import kafka.server.LogDirFailureChannel
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.utils.CoreUtils._
import kafka.utils.{Logging, Pool}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.errors.KafkaStorageException

import scala.collection.{immutable, mutable}

private[log] sealed trait LogCleaningState
private[log] case object LogCleaningInProgress extends LogCleaningState
private[log] case object LogCleaningAborted extends LogCleaningState
private[log] case object LogCleaningPaused extends LogCleaningState

/**
 *  Manage the state of each partition being cleaned.
 *  If a partition is to be cleaned, it enters the LogCleaningInProgress state.
 *  While a partition is being cleaned, it can be requested to be aborted and paused. Then the partition first enters
 *  the LogCleaningAborted state. Once the cleaning task is aborted, the partition enters the LogCleaningPaused state.
 *  While a partition is in the LogCleaningPaused state, it won't be scheduled for cleaning again, until cleaning is
 *  requested to be resumed.
 *
 *  管理清理中的分区状态.
 *  如果一个分区即将被清理, 那么它进入LogCleaningInProgress状态.
 *  如果一个分区正在被清理, 清理过程可以被终止或暂停. 在这种情况下, 分区会首先进入LogCleaningAborted状态. 一旦清理任务被终止,
 *  分区就会进入LogCleaningPaused状态.
 *  如果一个分区进入了LogCleaningPaused状态, 它不会被清理直到重新发起清理请求
 */
private[log] class LogCleanerManager(val logDirs: Seq[File],
                                     val logs: Pool[TopicPartition, Log],
                                     val logDirFailureChannel: LogDirFailureChannel) extends Logging with KafkaMetricsGroup {

  import LogCleanerManager._

  protected override def loggerName = classOf[LogCleaner].getName

  // package-private for testing
  private[log] val offsetCheckpointFile = "cleaner-offset-checkpoint"

  /* the offset checkpoints holding the last cleaned point for each log */
  // 此位移检查点为每个日志保存上一次的清理点
  @volatile private var checkpoints = logDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, offsetCheckpointFile), logDirFailureChannel))).toMap

  /* the set of logs currently being cleaned */
  //当前正在被清理的日志
  private val inProgress = mutable.HashMap[TopicPartition, LogCleaningState]()

  /* a global lock used to control all access to the in-progress set and the offset checkpoints */
  // 一个同步访问inProgress和checkpoints的全局锁
  private val lock = new ReentrantLock

  /* for coordinating the pausing and the cleaning of a partition */
  // 用来同步分区暂停和清理的条件量
  private val pausedCleaningCond = lock.newCondition()

  /* a gauge for tracking the cleanable ratio of the dirtiest log */
  // 用来记录最脏日志的可清理比例的指标
  @volatile private var dirtiestLogCleanableRatio = 0.0
  newGauge("max-dirty-percent", new Gauge[Int] { def value = (100 * dirtiestLogCleanableRatio).toInt })

  /* a gauge for tracking the time since the last log cleaner run, in milli seconds */
  // 用来记录上一次清理至今的时间, 单位为毫秒
  @volatile private var timeOfLastRun : Long = Time.SYSTEM.milliseconds
  newGauge("time-since-last-run-ms", new Gauge[Long] { def value = Time.SYSTEM.milliseconds - timeOfLastRun })

  /**
   * @return the position processed for all logs.
   * 返回 日志上一次清理的位置
   */
  def allCleanerCheckpoints: Map[TopicPartition, Long] = {
    inLock(lock) {
      checkpoints.values.flatMap(checkpoint => {
        try {
          checkpoint.read()
        } catch {
          case e: KafkaStorageException =>
            error(s"Failed to access checkpoint file ${checkpoint.file.getName} in dir ${checkpoint.file.getParentFile.getAbsolutePath}", e)
            Map.empty[TopicPartition, Long]
        }
      }).toMap
    }
  }

  /**
    * Package private for unit test. Get the cleaning state of the partition.
    */
  private[log] def cleaningState(tp: TopicPartition): Option[LogCleaningState] = {
    inLock(lock) {
      inProgress.get(tp)
    }
  }

  /**
    * Package private for unit test. Set the cleaning state of the partition.
    */
  private[log] def setCleaningState(tp: TopicPartition, state: LogCleaningState): Unit = {
    inLock(lock) {
      inProgress.put(tp, state)
    }
  }

   /**
    * Choose the log to clean next and add it to the in-progress set. We recompute this
    * each time from the full set of logs to allow logs to be dynamically added to the pool of logs
    * the log manager maintains.
    *
    * 选择需要进行清理的日志, 并把它加入到清理中的集合. 这里每次都会从全量日志中计算, 这样可以允许
    * 日志可以动态增加到日志池中
    */
  def grabFilthiestCompactedLog(time: Time): Option[LogToClean] = {
    inLock(lock) {
      val now = time.milliseconds
      this.timeOfLastRun = now
      val lastClean = allCleanerCheckpoints
      val dirtyLogs = logs.filter {
        //筛选出使用compact策略的日志
        case (_, log) => log.config.compact  // match logs that are marked as compacted
      }.filterNot {
        //去除正在进行清理的日志
        case (topicPartition, _) => inProgress.contains(topicPartition) // skip any logs already in-progress
      }.map {
        //create a LogToClean instance for each
        //为每个日志创建一个LogToClean实例
        case (topicPartition, log) =>
          //获取脏日志的初始位移和不可清理的脏日志部分初始位移
          val (firstDirtyOffset, firstUncleanableDirtyOffset) = LogCleanerManager.cleanableOffsets(log, topicPartition,
            lastClean, now)
          LogToClean(topicPartition, log, firstDirtyOffset, firstUncleanableDirtyOffset)
      }.filter(ltc => ltc.totalBytes > 0) // 跳过空日志

      //脏日志的最大可清理比例
      this.dirtiestLogCleanableRatio = if (dirtyLogs.nonEmpty) dirtyLogs.max.cleanableRatio else 0

      // and must meet the minimum threshold for dirty byte ratio
      // 保证可清理比例大于配置的最小清理比例
      val cleanableLogs = dirtyLogs.filter(ltc => ltc.cleanableRatio > ltc.log.config.minCleanableRatio)
      if(cleanableLogs.isEmpty) {
        None
      } else {
        //找出最脏的日志
        val filthiest = cleanableLogs.max

        //加到正在进行中的集合里面去
        inProgress.put(filthiest.topicPartition, LogCleaningInProgress)

        //返回结果
        Some(filthiest)
      }
    }
  }

  /**
    * Find any logs that have compact and delete enabled
    */
  def deletableLogs(): Iterable[(TopicPartition, Log)] = {
    inLock(lock) {
      val toClean = logs.filter { case (topicPartition, log) =>
        !inProgress.contains(topicPartition) && isCompactAndDelete(log)
      }
      toClean.foreach { case (tp, _) => inProgress.put(tp, LogCleaningInProgress) }
      toClean
    }

  }

  /**
   *  Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
   *  the partition is aborted.
   *  This is implemented by first abortAndPausing and then resuming the cleaning of the partition.
   */
  def abortCleaning(topicPartition: TopicPartition) {
    inLock(lock) {
      //终止且暂停分区的清理
      abortAndPauseCleaning(topicPartition)
      //将分区的状态重新恢复为可清理的状态
      resumeCleaning(topicPartition)
    }
    info(s"The cleaning for partition $topicPartition is aborted")
  }

  /**
   *  Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
   *  This call blocks until the cleaning of the partition is aborted and paused.
   *  1. If the partition is not in progress, mark it as paused.
   *  2. Otherwise, first mark the state of the partition as aborted.
   *  3. The cleaner thread checks the state periodically and if it sees the state of the partition is aborted, it
   *     throws a LogCleaningAbortedException to stop the cleaning task.
   *  4. When the cleaning task is stopped, doneCleaning() is called, which sets the state of the partition as paused.
   *  5. abortAndPauseCleaning() waits until the state of the partition is changed to paused.
   *
   *  终止一个特定分区的清理(如果它在清理过程中的话), 并且暂停将来的清理. 这个调用会阻塞直到该分区清理过程被终止.
   */
  def abortAndPauseCleaning(topicPartition: TopicPartition) {
    inLock(lock) {
      inProgress.get(topicPartition) match {
        case None =>
          //如果该分区没有在清理过程中, 那么标记其为"暂停"
          inProgress.put(topicPartition, LogCleaningPaused)
        case Some(state) =>
          state match {
            case LogCleaningInProgress =>
              //如果该分区正在清理过程中, 则先标记为"终止"
              inProgress.put(topicPartition, LogCleaningAborted)
            case LogCleaningPaused =>
            case s =>
              //如果该分区已经被标记为"暂停"或其他状态, 那么抛出异常
              throw new IllegalStateException(s"Compaction for partition $topicPartition cannot be aborted and paused since it is in $s state.")
          }
      }
      //定期检查该分区状态是否为"暂停"直到其为暂停位置
      while (!isCleaningInState(topicPartition, LogCleaningPaused))
        pausedCleaningCond.await(100, TimeUnit.MILLISECONDS)
    }
    info(s"The cleaning for partition $topicPartition is aborted and paused")
  }

  /**
   *  Resume the cleaning of a paused partition. This call blocks until the cleaning of a partition is resumed.
   */
  def resumeCleaning(topicPartition: TopicPartition) {
    inLock(lock) {
      inProgress.get(topicPartition) match {
        case None =>
          throw new IllegalStateException(s"Compaction for partition $topicPartition cannot be resumed since it is not paused.")
        case Some(state) =>
          state match {
            case LogCleaningPaused =>
              inProgress.remove(topicPartition)
            case s =>
              throw new IllegalStateException(s"Compaction for partition $topicPartition cannot be resumed since it is in $s state.")
          }
      }
    }
    info(s"Compaction for partition $topicPartition is resumed")
  }

  /**
   *  Check if the cleaning for a partition is in a particular state. The caller is expected to hold lock while making the call.
   */
  private def isCleaningInState(topicPartition: TopicPartition, expectedState: LogCleaningState): Boolean = {
    inProgress.get(topicPartition) match {
      case None => false
      case Some(state) =>
        if (state == expectedState)
          true
        else
          false
    }
  }

  /**
   *  Check if the cleaning for a partition is aborted. If so, throw an exception.
   */
  def checkCleaningAborted(topicPartition: TopicPartition) {
    inLock(lock) {
      if (isCleaningInState(topicPartition, LogCleaningAborted))
        throw new LogCleaningAbortedException()
    }
  }

  /**
   * 更新检查点文件
   * @param dataDir
   * @param update
   */
  def updateCheckpoints(dataDir: File, update: Option[(TopicPartition,Long)]) {
    inLock(lock) {
      val checkpoint = checkpoints(dataDir)
      if (checkpoint != null) {
        try {
          val existing = checkpoint.read().filterKeys(logs.keys) ++ update
          checkpoint.write(existing)
        } catch {
          case e: KafkaStorageException =>
            error(s"Failed to access checkpoint file ${checkpoint.file.getName} in dir ${checkpoint.file.getParentFile.getAbsolutePath}", e)
        }
      }
    }
  }

  def alterCheckpointDir(topicPartition: TopicPartition, sourceLogDir: File, destLogDir: File): Unit = {
    inLock(lock) {
      try {
        checkpoints.get(sourceLogDir).flatMap(_.read().get(topicPartition)) match {
          case Some(offset) =>
            // Remove this partition from the checkpoint file in the source log directory
            // 在原日志目录的检查点文件中删除此分区位移
            updateCheckpoints(sourceLogDir, None)
            // Add offset for this partition to the checkpoint file in the source log directory
            // 在新的日志目录的检查点文件中增加此分区的位移
            updateCheckpoints(destLogDir, Option(topicPartition, offset))
          case None =>
        }
      } catch {
        case e: KafkaStorageException =>
          error(s"Failed to access checkpoint file in dir ${sourceLogDir.getAbsolutePath}", e)
      }
    }
  }

  def handleLogDirFailure(dir: String) {
    info(s"Stopping cleaning logs in dir $dir")
    inLock(lock) {
      checkpoints = checkpoints.filterKeys(_.getAbsolutePath != dir)
    }
  }

  def maybeTruncateCheckpoint(dataDir: File, topicPartition: TopicPartition, offset: Long) {
    inLock(lock) {
      if (logs.get(topicPartition).config.compact) {
        val checkpoint = checkpoints(dataDir)
        if (checkpoint != null) {
          val existing = checkpoint.read()
          if (existing.getOrElse(topicPartition, 0L) > offset)
            checkpoint.write(existing + (topicPartition -> offset))
        }
      }
    }
  }

  /**
   * Save out the endOffset and remove the given log from the in-progress set, if not aborted.
   */
  def doneCleaning(topicPartition: TopicPartition, dataDir: File, endOffset: Long) {
    inLock(lock) {
      inProgress.get(topicPartition) match {
        case Some(LogCleaningInProgress) =>
          updateCheckpoints(dataDir, Option(topicPartition, endOffset))
          inProgress.remove(topicPartition)
        case Some(LogCleaningAborted) =>
          inProgress.put(topicPartition, LogCleaningPaused)
          pausedCleaningCond.signalAll()
        case None =>
          throw new IllegalStateException(s"State for partition $topicPartition should exist.")
        case s =>
          throw new IllegalStateException(s"In-progress partition $topicPartition cannot be in $s state.")
      }
    }
  }

  def doneDeleting(topicPartition: TopicPartition): Unit = {
    inLock(lock) {
      inProgress.get(topicPartition) match {
        case Some(LogCleaningInProgress) =>
          inProgress.remove(topicPartition)
        case Some(LogCleaningAborted) =>
          inProgress.put(topicPartition, LogCleaningPaused)
          pausedCleaningCond.signalAll()
        case None =>
          throw new IllegalStateException(s"State for partition $topicPartition should exist.")
        case s =>
          throw new IllegalStateException(s"In-progress partition $topicPartition cannot be in $s state.")
      }
    }
  }
}

private[log] object LogCleanerManager extends Logging {

  def isCompactAndDelete(log: Log): Boolean = {
    log.config.compact && log.config.delete
  }


  /**
    * Returns the range of dirty offsets that can be cleaned.
    *
    * @param log the log
    * @param lastClean the map of checkpointed offsets
    * @param now the current time in milliseconds of the cleaning operation
    * @return the lower (inclusive) and upper (exclusive) offsets
    *
    * 获取可清理的脏日志范围
    * 参数 log: 日志
    * 参数 lastClean: 上一次清理位移
    * 参数 now: 当前时间, 单位毫秒
    * 返回 可清理的脏日志范围下限(包括)和上限(不包括)
    */
  def cleanableOffsets(log: Log, topicPartition: TopicPartition, lastClean: immutable.Map[TopicPartition, Long], now: Long): (Long, Long) = {

    // the checkpointed offset, ie., the first offset of the next dirty segment
    // 检查点位移, 也就是脏日志的初始位移
    val lastCleanOffset: Option[Long] = lastClean.get(topicPartition)

    // If the log segments are abnormally truncated and hence the checkpointed offset is no longer valid;
    // reset to the log starting offset and log the error
    // 如果日志段意外被截断(即检查点位移小于当前日志初始位移), 那么检查点位移失效, 重新设置为日志初始位移并记录异常
    val logStartOffset = log.logSegments.head.baseOffset
    val firstDirtyOffset = {
      val offset = lastCleanOffset.getOrElse(logStartOffset)
      if (offset < logStartOffset) {
        // don't bother with the warning if compact and delete are enabled.
        if (!isCompactAndDelete(log))
          warn(s"Resetting first dirty offset of ${log.name} to log start offset $logStartOffset since the checkpointed offset $offset is invalid.")
        logStartOffset
      } else {
        offset
      }
    }

    //获取compact的延后时间
    val compactionLagMs = math.max(log.config.compactionLagMs, 0L)

    // find first segment that cannot be cleaned
    // neither the active segment, nor segments with any messages closer to the head of the log than the minimum compaction lag time
    // may be cleaned
    // 获取第一个不能被清理的日志段, 该日志段或者正在追加日志, 或者消息时间戳在所设置的延后时间之内
    val firstUncleanableDirtyOffset: Long = Seq(

      // we do not clean beyond the first unstable offset
      // 清理操作不会越过unstable位移(也就是未完成事务或未复制事务的最小位移)
      log.firstUnstableOffset.map(_.messageOffset),

      // the active segment is always uncleanable
      // 当前正在追加消息的日志段总是不可清理的
      Option(log.activeSegment.baseOffset),

      // the first segment whose largest message timestamp is within a minimum time lag from now
      // 获取第一个最大消息时间戳在延后时间之内的日志段
      if (compactionLagMs > 0) {
        // dirty log segments
        val dirtyNonActiveSegments = log.logSegments(firstDirtyOffset, log.activeSegment.baseOffset)
        dirtyNonActiveSegments.find { s =>
          val isUncleanable = s.largestTimestamp > now - compactionLagMs
          debug(s"Checking if log segment may be cleaned: log='${log.name}' segment.baseOffset=${s.baseOffset} segment.largestTimestamp=${s.largestTimestamp}; now - compactionLag=${now - compactionLagMs}; is uncleanable=$isUncleanable")
          isUncleanable
        }.map(_.baseOffset)
      } else None

    //然后取上面三者的最小值作为不可清理的脏日志初始位移
    ).flatten.min

    debug(s"Finding range of cleanable offsets for log=${log.name} topicPartition=$topicPartition. Last clean offset=$lastCleanOffset now=$now => firstDirtyOffset=$firstDirtyOffset firstUncleanableOffset=$firstUncleanableDirtyOffset activeSegment.baseOffset=${log.activeSegment.baseOffset}")

    (firstDirtyOffset, firstUncleanableDirtyOffset)
  }
}

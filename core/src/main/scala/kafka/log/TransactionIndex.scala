/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, StandardOpenOption}

import kafka.utils.{Logging, nonthreadsafe}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable.ListBuffer

private[log] case class TxnIndexSearchResult(abortedTransactions: List[AbortedTxn], isComplete: Boolean)

/**
 * The transaction index maintains metadata about the aborted transactions for each segment. This includes
 * the start and end offsets for the aborted transactions and the last stable offset (LSO) at the time of
 * the abort. This index is used to find the aborted transactions in the range of a given fetch request at
 * the READ_COMMITTED isolation level.
 *
 * There is at most one transaction index for each log segment. The entries correspond to the transactions
 * whose commit markers were written in the corresponding log segment. Note, however, that individual transactions
 * may span multiple segments. Recovering the index therefore requires scanning the earlier segments in
 * order to find the start of the transactions.
 *
 * 事务索引包含了已回滚事务的元数据, 这包括已回滚事务的开始和结束位移, 以及回滚事务时的最后stable位移(LSO).
 * 在消费者以READ_COMMITTED级别拉取消息时, 这个索引用来查询拉取范围内的已回滚事务.
 *
 * 每个日志段最多只有一个事务索引, 索引条目映射日志段中的事务,这些事务的commit标记记录在日志段中. 需要注意的是, 单个事务可能
 * 横跨若干个日志段, 因此恢复索引时需要扫描更早的日志段来找到事务的开始.
 */
@nonthreadsafe
class TransactionIndex(val startOffset: Long, @volatile var file: File) extends Logging {
  // note that the file is not created until we need it
  @volatile private var maybeChannel: Option[FileChannel] = None
  private var lastOffset: Option[Long] = None

  if (file.exists)
    openChannel()

  /**
   * 增加回滚事务的索引
   * @param abortedTxn
   */
  def append(abortedTxn: AbortedTxn): Unit = {
    // 如果位移小于等于之前的位移, 则抛出IllegalArgumentException位移
    lastOffset.foreach { offset =>
      if (offset >= abortedTxn.lastOffset)
        throw new IllegalArgumentException("The last offset of appended transactions must increase sequentially")
    }
    //记录最新的位移
    lastOffset = Some(abortedTxn.lastOffset)

    //写入channel
    Utils.writeFully(channel, abortedTxn.buffer.duplicate())
  }

  def flush(): Unit = maybeChannel.foreach(_.force(true))

  /**
   * Delete this index.
   *
   * @throws IOException if deletion fails due to an I/O error
   * @return `true` if the file was deleted by this method; `false` if the file could not be deleted because it did
   *         not exist
   */
  def deleteIfExists(): Boolean = {
    close()
    Files.deleteIfExists(file.toPath)
  }

  private def channel: FileChannel = {
    maybeChannel match {
      case Some(channel) => channel
      case None => openChannel()
    }
  }

  private def openChannel(): FileChannel = {
    val channel = FileChannel.open(file.toPath, StandardOpenOption.READ, StandardOpenOption.WRITE,
      StandardOpenOption.CREATE)
    maybeChannel = Some(channel)
    channel.position(channel.size)
    channel
  }

  /**
   * Remove all the entries from the index. Unlike `AbstractIndex`, this index is not resized ahead of time.
   */
  def reset(): Unit = {
    maybeChannel.foreach(_.truncate(0))
    lastOffset = None
  }

  def close(): Unit = {
    maybeChannel.foreach(_.close())
    maybeChannel = None
  }

  def renameTo(f: File): Unit = {
    try {
      if (file.exists)
        Utils.atomicMoveWithFallback(file.toPath, f.toPath)
    } finally file = f
  }

  def truncateTo(offset: Long): Unit = {
    val buffer = ByteBuffer.allocate(AbortedTxn.TotalSize)
    var newLastOffset: Option[Long] = None
    for ((abortedTxn, position) <- iterator(() => buffer)) {
      if (abortedTxn.lastOffset >= offset) {
        channel.truncate(position)
        lastOffset = newLastOffset
        return
      }
      newLastOffset = Some(abortedTxn.lastOffset)
    }
  }

  /**
   * 获取扫描此事务索引的迭代器
   *
   * @param allocate
   * @return
   */
  private def iterator(allocate: () => ByteBuffer = () => ByteBuffer.allocate(AbortedTxn.TotalSize)): Iterator[(AbortedTxn, Int)] = {
    maybeChannel match {
      case None => Iterator.empty
      case Some(channel) =>
        var position = 0

        new Iterator[(AbortedTxn, Int)] {
          override def hasNext: Boolean = channel.position - position >= AbortedTxn.TotalSize

          override def next(): (AbortedTxn, Int) = {
            try {
              //分配buffer
              val buffer = allocate()
              //读取事务索引
              Utils.readFully(channel, buffer, position)
              buffer.flip()

              val abortedTxn = new AbortedTxn(buffer)
              if (abortedTxn.version > AbortedTxn.CurrentVersion)
                throw new KafkaException(s"Unexpected aborted transaction version ${abortedTxn.version}, " +
                  s"current version is ${AbortedTxn.CurrentVersion}")
              val nextEntry = (abortedTxn, position)
              position += AbortedTxn.TotalSize
              nextEntry
            } catch {
              case e: IOException =>
                // We received an unexpected error reading from the index file. We propagate this as an
                // UNKNOWN error to the consumer, which will cause it to retry the fetch.
                throw new KafkaException(s"Failed to read from the transaction index $file", e)
            }
          }
        }
    }
  }

  def allAbortedTxns: List[AbortedTxn] = {
    iterator().map(_._1).toList
  }

  /**
   * Collect all aborted transactions which overlap with a given fetch range.
   *
   * @param fetchOffset Inclusive first offset of the fetch range
   * @param upperBoundOffset Exclusive last offset in the fetch range
   * @return An object containing the aborted transactions and whether the search needs to continue
   *         into the next log segment.
   *
   * 收集所有与指定拉取范围重叠的已回滚事务
   *
   * 参数 fetchOffset: 拉取范围的起始位移(包含)
   * 参数 upperBoundOffset: 拉取范围的最后位移(不包含)
   * 返回 一个实例, 该实例包含回滚事务和指示是否需要在下一个日志段继续搜索的指示器
   */
  def collectAbortedTxns(fetchOffset: Long, upperBoundOffset: Long): TxnIndexSearchResult = {
    val abortedTransactions = ListBuffer.empty[AbortedTxn]
    for ((abortedTxn, _) <- iterator()) {
      if (abortedTxn.lastOffset >= fetchOffset && abortedTxn.firstOffset < upperBoundOffset)
        abortedTransactions += abortedTxn

      if (abortedTxn.lastStableOffset >= upperBoundOffset)
        return TxnIndexSearchResult(abortedTransactions.toList, isComplete = true)
    }
    TxnIndexSearchResult(abortedTransactions.toList, isComplete = false)
  }

  /**
   * Do a basic sanity check on this index to detect obvious problems.
   *
   * @throws CorruptIndexException if any problems are found.
   */
  def sanityCheck(): Unit = {
    val buffer = ByteBuffer.allocate(AbortedTxn.TotalSize)
    for ((abortedTxn, _) <- iterator(() => buffer)) {
      if (abortedTxn.lastOffset < startOffset)
        throw new CorruptIndexException(s"Last offset of aborted transaction $abortedTxn is less than start offset " +
          s"$startOffset")
    }
  }

}

private[log] object AbortedTxn {
  val VersionOffset = 0
  val VersionSize = 2
  val ProducerIdOffset = VersionOffset + VersionSize
  val ProducerIdSize = 8
  val FirstOffsetOffset = ProducerIdOffset + ProducerIdSize
  val FirstOffsetSize = 8
  val LastOffsetOffset = FirstOffsetOffset + FirstOffsetSize
  val LastOffsetSize = 8
  val LastStableOffsetOffset = LastOffsetOffset + LastOffsetSize
  val LastStableOffsetSize = 8
  val TotalSize = LastStableOffsetOffset + LastStableOffsetSize

  val CurrentVersion: Short = 0
}

private[log] class AbortedTxn(val buffer: ByteBuffer) {
  import AbortedTxn._

  def this(producerId: Long,
           firstOffset: Long,
           lastOffset: Long,
           lastStableOffset: Long) = {
    this(ByteBuffer.allocate(AbortedTxn.TotalSize))
    buffer.putShort(CurrentVersion)
    buffer.putLong(producerId)
    buffer.putLong(firstOffset)
    buffer.putLong(lastOffset)
    buffer.putLong(lastStableOffset)
    buffer.flip()
  }

  def this(completedTxn: CompletedTxn, lastStableOffset: Long) =
    this(completedTxn.producerId, completedTxn.firstOffset, completedTxn.lastOffset, lastStableOffset)

  def version: Short = buffer.get(VersionOffset)

  def producerId: Long = buffer.getLong(ProducerIdOffset)

  def firstOffset: Long = buffer.getLong(FirstOffsetOffset)

  def lastOffset: Long = buffer.getLong(LastOffsetOffset)

  def lastStableOffset: Long = buffer.getLong(LastStableOffsetOffset)

  def asAbortedTransaction: AbortedTransaction = new AbortedTransaction(producerId, firstOffset)

  override def toString: String =
    s"AbortedTxn(version=$version, producerId=$producerId, firstOffset=$firstOffset, " +
      s"lastOffset=$lastOffset, lastStableOffset=$lastStableOffset)"

  override def equals(any: Any): Boolean = {
    any match {
      case that: AbortedTxn => this.buffer.equals(that.buffer)
      case _ => false
    }
  }

  override def hashCode(): Int = buffer.hashCode
}

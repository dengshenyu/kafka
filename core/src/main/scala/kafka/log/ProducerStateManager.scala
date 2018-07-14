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

import java.io._
import java.nio.ByteBuffer
import java.nio.file.Files

import kafka.log.Log.offsetFromFile
import kafka.server.LogOffsetMetadata
import kafka.utils.{Logging, nonthreadsafe, threadsafe}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.protocol.types._
import org.apache.kafka.common.record.{ControlRecordType, EndTransactionMarker, RecordBatch}
import org.apache.kafka.common.utils.{ByteUtils, Crc32C}

import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}

class CorruptSnapshotException(msg: String) extends KafkaException(msg)


// ValidationType and its subtypes define the extent of the validation to perform on a given ProducerAppendInfo instance
private[log] sealed trait ValidationType
private[log] object ValidationType {

  /**
    * This indicates no validation should be performed on the incoming append. This is the case for all appends on
    * a replica, as well as appends when the producer state is being built from the log.
    */
  case object None extends ValidationType

  /**
    * We only validate the epoch (and not the sequence numbers) for offset commit requests coming from the transactional
    * producer. These appends will not have sequence numbers, so we can't validate them.
    */
  case object EpochOnly extends ValidationType

  /**
    * Perform the full validation. This should be used fo regular produce requests coming to the leader.
    */
  case object Full extends ValidationType
}

private[log] case class TxnMetadata(producerId: Long, var firstOffset: LogOffsetMetadata, var lastOffset: Option[Long] = None) {
  def this(producerId: Long, firstOffset: Long) = this(producerId, LogOffsetMetadata(firstOffset))

  override def toString: String = {
    "TxnMetadata(" +
      s"producerId=$producerId, " +
      s"firstOffset=$firstOffset, " +
      s"lastOffset=$lastOffset)"
  }
}

private[log] object ProducerStateEntry {
  private[log] val NumBatchesToRetain = 5
  def empty(producerId: Long) = new ProducerStateEntry(producerId, mutable.Queue[BatchMetadata](), RecordBatch.NO_PRODUCER_EPOCH, -1, None)
}

private[log] case class BatchMetadata(lastSeq: Int, lastOffset: Long, offsetDelta: Int, timestamp: Long) {
  def firstSeq = lastSeq - offsetDelta
  def firstOffset = lastOffset - offsetDelta

  override def toString: String = {
    "BatchMetadata(" +
      s"firstSeq=$firstSeq, " +
      s"lastSeq=$lastSeq, " +
      s"firstOffset=$firstOffset, " +
      s"lastOffset=$lastOffset, " +
      s"timestamp=$timestamp)"
  }
}

// the batchMetadata is ordered such that the batch with the lowest sequence is at the head of the queue while the
// batch with the highest sequence is at the tail of the queue. We will retain at most ProducerStateEntry.NumBatchesToRetain
// elements in the queue. When the queue is at capacity, we remove the first element to make space for the incoming batch.
// batchMetadata是有序的队列, 低序列号的在前, 高序列号的在后. 使用这个结构来保持队列中最多有ProducerStateEntry.NumBatchesToRetain
// 个元素, 当超过此阈值时, 删除老的元素同时增加新的元素.
private[log] class ProducerStateEntry(val producerId: Long,
                                      val batchMetadata: mutable.Queue[BatchMetadata],
                                      var producerEpoch: Short,
                                      var coordinatorEpoch: Int,
                                      var currentTxnFirstOffset: Option[Long]) {

  def firstSeq: Int = if (isEmpty) RecordBatch.NO_SEQUENCE else batchMetadata.front.firstSeq

  def firstOffset: Long = if (isEmpty) -1L else batchMetadata.front.firstOffset

  def lastSeq: Int = if (isEmpty) RecordBatch.NO_SEQUENCE else batchMetadata.last.lastSeq

  def lastDataOffset: Long = if (isEmpty) -1L else batchMetadata.last.lastOffset

  def lastTimestamp = if (isEmpty) RecordBatch.NO_TIMESTAMP else batchMetadata.last.timestamp

  def lastOffsetDelta : Int = if (isEmpty) 0 else batchMetadata.last.offsetDelta

  def isEmpty: Boolean = batchMetadata.isEmpty

  def addBatch(producerEpoch: Short, lastSeq: Int, lastOffset: Long, offsetDelta: Int, timestamp: Long): Unit = {
    //处理epoch更新
    maybeUpdateEpoch(producerEpoch)
    //更新batch元数据队列
    addBatchMetadata(BatchMetadata(lastSeq, lastOffset, offsetDelta, timestamp))
  }

  /**
   * 处理epoch更新
   * @param producerEpoch
   * @return
   */
  def maybeUpdateEpoch(producerEpoch: Short): Boolean = {
    if (this.producerEpoch != producerEpoch) {
      batchMetadata.clear()
      this.producerEpoch = producerEpoch
      true
    } else {
      false
    }
  }

    /**
     * 更新最近追加的batch元数据队列
     */
  private def addBatchMetadata(batch: BatchMetadata): Unit = {
    if (batchMetadata.size == ProducerStateEntry.NumBatchesToRetain)
      batchMetadata.dequeue()
    batchMetadata.enqueue(batch)
  }

  def update(nextEntry: ProducerStateEntry): Unit = {
    maybeUpdateEpoch(nextEntry.producerEpoch)
    while (nextEntry.batchMetadata.nonEmpty)
      addBatchMetadata(nextEntry.batchMetadata.dequeue())
    this.coordinatorEpoch = nextEntry.coordinatorEpoch
    this.currentTxnFirstOffset = nextEntry.currentTxnFirstOffset
  }

  def removeBatchesOlderThan(offset: Long): Unit = batchMetadata.dropWhile(_.lastOffset < offset)

  /**
   * 查找缓存中拥有相同序列号区间的batch
   * @param batch
   * @return
   */
  def findDuplicateBatch(batch: RecordBatch): Option[BatchMetadata] = {
    if (batch.producerEpoch != producerEpoch)
       None
    else
      batchWithSequenceRange(batch.baseSequence, batch.lastSequence)
  }

  // Return the batch metadata of the cached batch having the exact sequence range, if any.
  // 返回拥有相同序列号区间的batch元数据
  def batchWithSequenceRange(firstSeq: Int, lastSeq: Int): Option[BatchMetadata] = {
    val duplicate = batchMetadata.filter { metadata =>
      firstSeq == metadata.firstSeq && lastSeq == metadata.lastSeq
    }
    duplicate.headOption
  }

  override def toString: String = {
    "ProducerStateEntry(" +
      s"producerId=$producerId, " +
      s"producerEpoch=$producerEpoch, " +
      s"currentTxnFirstOffset=$currentTxnFirstOffset, " +
      s"coordinatorEpoch=$coordinatorEpoch, " +
      s"batchMetadata=$batchMetadata"
  }
}

/**
 * This class is used to validate the records appended by a given producer before they are written to the log.
 * It is initialized with the producer's state after the last successful append, and transitively validates the
 * sequence numbers and epochs of each new record. Additionally, this class accumulates transaction metadata
 * as the incoming records are validated.
 *
 * @param producerId The id of the producer appending to the log
 * @param currentEntry  The current entry associated with the producer id which contains metadata for a fixed number of
 *                      the most recent appends made by the producer. Validation of the first incoming append will
 *                      be made against the latest append in the current entry. New appends will replace older appends
 *                      in the current entry so that the space overhead is constant.
 * @param validationType Indicates the extent of validation to perform on the appends on this instance. Offset commits
 *                       coming from the producer should have ValidationType.EpochOnly. Appends which aren't from a client
 *                       should have ValidationType.None. Appends coming from a client for produce requests should have
 *                       ValidationType.Full.
 * 此类用来验证生产者追加记录的合法性. 此类会在最后一次成功追加后进行初始化, 并不断验证及更新此后每个记录的序列号和epoch.
 * 另外, 此类在验证接收到的记录后会收集事务信息
 * 参数 producerId: 追加日志的生产者ID
 * 参数 currentEntry: 包含此生产者最近追加的记录元信息, 新的追加请求会与此结构中的最后追加信息进行比较进行验证, 而且新的追加会替代
 *                    此结构中的老数据以保持常量的空间
 * 参数 validationType: 指定校验的范围. 生产者的位移提交请求为ValidationType.EpochOnly, 非client的追加请求为ValidationType.None,
 *                    client的追加请求为ValidationType.Full
 */
private[log] class ProducerAppendInfo(val producerId: Long,
                                      val currentEntry: ProducerStateEntry,
                                      val validationType: ValidationType) {
  private val transactions = ListBuffer.empty[TxnMetadata]
  private val updatedEntry = ProducerStateEntry.empty(producerId)

  updatedEntry.producerEpoch = currentEntry.producerEpoch
  updatedEntry.coordinatorEpoch = currentEntry.coordinatorEpoch
  updatedEntry.currentTxnFirstOffset = currentEntry.currentTxnFirstOffset

  /**
   * 校验生产者状态
   * @param producerEpoch
   * @param firstSeq
   */
  private def maybeValidateAppend(producerEpoch: Short, firstSeq: Int) = {
    validationType match {
      case ValidationType.None =>

      case ValidationType.EpochOnly =>
        checkProducerEpoch(producerEpoch)

      case ValidationType.Full =>
        checkProducerEpoch(producerEpoch)
        checkSequence(producerEpoch, firstSeq)
    }
  }

  /**
   * 检查生产者的epoch, 如果比之前的要老, 那么抛出ProducerFencedException异常
   * @param producerEpoch
   */
  private def checkProducerEpoch(producerEpoch: Short): Unit = {
    if (producerEpoch < updatedEntry.producerEpoch) {
      throw new ProducerFencedException(s"Producer's epoch is no longer valid. There is probably another producer " +
        s"with a newer epoch. $producerEpoch (request epoch), ${updatedEntry.producerEpoch} (server epoch)")
    }
  }

  private def checkSequence(producerEpoch: Short, appendFirstSeq: Int): Unit = {
    if (producerEpoch != updatedEntry.producerEpoch) {
      if (appendFirstSeq != 0) {
        if (updatedEntry.producerEpoch != RecordBatch.NO_PRODUCER_EPOCH) {
          throw new OutOfOrderSequenceException(s"Invalid sequence number for new epoch: $producerEpoch " +
            s"(request epoch), $appendFirstSeq (seq. number)")
        } else {
          throw new UnknownProducerIdException(s"Found no record of producerId=$producerId on the broker. It is possible " +
            s"that the last message with the producerId=$producerId has been removed due to hitting the retention limit.")
        }
      }
    } else {
      val currentLastSeq = if (!updatedEntry.isEmpty)
        updatedEntry.lastSeq
      else if (producerEpoch == currentEntry.producerEpoch)
        currentEntry.lastSeq
      else
        RecordBatch.NO_SEQUENCE

      if (currentLastSeq == RecordBatch.NO_SEQUENCE && appendFirstSeq != 0) {
        // the epoch was bumped by a control record, so we expect the sequence number to be reset
        throw new OutOfOrderSequenceException(s"Out of order sequence number for producerId $producerId: found $appendFirstSeq " +
          s"(incoming seq. number), but expected 0")
      } else if (!inSequence(currentLastSeq, appendFirstSeq)) {
        throw new OutOfOrderSequenceException(s"Out of order sequence number for producerId $producerId: $appendFirstSeq " +
          s"(incoming seq. number), $currentLastSeq (current end sequence number)")
      }
    }
  }

  private def inSequence(lastSeq: Int, nextSeq: Int): Boolean = {
    nextSeq == lastSeq + 1L || (nextSeq == 0 && lastSeq == Int.MaxValue)
  }

  def append(batch: RecordBatch): Option[CompletedTxn] = {
    if (batch.isControlBatch) {
      //事务结束消息
      val record = batch.iterator.next()
      val endTxnMarker = EndTransactionMarker.deserialize(record)
      val completedTxn = appendEndTxnMarker(endTxnMarker, batch.producerEpoch, batch.baseOffset, record.timestamp)
      Some(completedTxn)
    } else {
      //非控制消息
      append(batch.producerEpoch, batch.baseSequence, batch.lastSequence, batch.maxTimestamp, batch.lastOffset,
        batch.isTransactional)
      None
    }
  }

  /**
   * 追加普通消息
   * @param epoch
   * @param firstSeq
   * @param lastSeq
   * @param lastTimestamp
   * @param lastOffset
   * @param isTransactional
   */
  def append(epoch: Short,
             firstSeq: Int,
             lastSeq: Int,
             lastTimestamp: Long,
             lastOffset: Long,
             isTransactional: Boolean): Unit = {
    //校验消息
    maybeValidateAppend(epoch, firstSeq)
    //根据新的batch更新此生产者新增数据的信息
    updatedEntry.addBatch(epoch, lastSeq, lastOffset, lastSeq - firstSeq, lastTimestamp)

    updatedEntry.currentTxnFirstOffset match {
      case Some(_) if !isTransactional =>
        // 如果在事务过程中接收到一个非事务的消息则抛出InvalidTxnStateException异常
        throw new InvalidTxnStateException(s"Expected transactional write from producer $producerId")

      case None if isTransactional =>
        // 开始一个新的事务
        val firstOffset = lastOffset - (lastSeq - firstSeq)
        updatedEntry.currentTxnFirstOffset = Some(firstOffset)
        transactions += new TxnMetadata(producerId, firstOffset)

      case _ => // nothing to do
    }
  }

  /**
   * 追加事务结束消息
   *
   * @param endTxnMarker
   * @param producerEpoch
   * @param offset
   * @param timestamp
   * @return
   */
  def appendEndTxnMarker(endTxnMarker: EndTransactionMarker,
                         producerEpoch: Short,
                         offset: Long,
                         timestamp: Long): CompletedTxn = {
    //检查生产者epoch
    checkProducerEpoch(producerEpoch)

    //检查coordinatorEpoch
    if (updatedEntry.coordinatorEpoch > endTxnMarker.coordinatorEpoch)
      throw new TransactionCoordinatorFencedException(s"Invalid coordinator epoch: ${endTxnMarker.coordinatorEpoch} " +
        s"(zombie), ${updatedEntry.coordinatorEpoch} (current)")

    //更新epoch
    updatedEntry.maybeUpdateEpoch(producerEpoch)

    val firstOffset = updatedEntry.currentTxnFirstOffset match {
      case Some(txnFirstOffset) => txnFirstOffset
      case None =>
        transactions += new TxnMetadata(producerId, offset)
        offset
    }

    updatedEntry.currentTxnFirstOffset = None
    updatedEntry.coordinatorEpoch = endTxnMarker.coordinatorEpoch
    CompletedTxn(producerId, firstOffset, offset, endTxnMarker.controlType == ControlRecordType.ABORT)
  }

  def toEntry: ProducerStateEntry = updatedEntry

  def startedTransactions: List[TxnMetadata] = transactions.toList

  def maybeCacheTxnFirstOffsetMetadata(logOffsetMetadata: LogOffsetMetadata): Unit = {
    // we will cache the log offset metadata if it corresponds to the starting offset of
    // the last transaction that was started. This is optimized for leader appends where it
    // is only possible to have one transaction started for each log append, and the log
    // offset metadata will always match in that case since no data from other producers
    // is mixed into the append
    // 如果日志位移与最后一个事务的起始位移相同, 那么缓存此日志位移元数据. 这是leader追加的一个优化,
    // 因为只允许在每次追加时开始一个事务, 而且日志位移元数据总是匹配上的因为没有来源于其他生产者的数
    // 据混合在其中
    transactions.headOption.foreach { txn =>
      if (txn.firstOffset.messageOffset == logOffsetMetadata.messageOffset)
        txn.firstOffset = logOffsetMetadata
    }
  }

  override def toString: String = {
    "ProducerAppendInfo(" +
      s"producerId=$producerId, " +
      s"producerEpoch=${updatedEntry.producerEpoch}, " +
      s"firstSequence=${updatedEntry.firstSeq}, " +
      s"lastSequence=${updatedEntry.lastSeq}, " +
      s"currentTxnFirstOffset=${updatedEntry.currentTxnFirstOffset}, " +
      s"coordinatorEpoch=${updatedEntry.coordinatorEpoch}, " +
      s"startedTransactions=$transactions)"
  }
}

object ProducerStateManager {
  private val ProducerSnapshotVersion: Short = 1
  private val VersionField = "version"
  private val CrcField = "crc"
  private val ProducerIdField = "producer_id"
  private val LastSequenceField = "last_sequence"
  private val ProducerEpochField = "epoch"
  private val LastOffsetField = "last_offset"
  private val OffsetDeltaField = "offset_delta"
  private val TimestampField = "timestamp"
  private val ProducerEntriesField = "producer_entries"
  private val CoordinatorEpochField = "coordinator_epoch"
  private val CurrentTxnFirstOffsetField = "current_txn_first_offset"

  private val VersionOffset = 0
  private val CrcOffset = VersionOffset + 2
  private val ProducerEntriesOffset = CrcOffset + 4

  //生产者快照中每一个生产者状态条目的格式
  val ProducerSnapshotEntrySchema = new Schema(
    new Field(ProducerIdField, Type.INT64, "The producer ID"),
    new Field(ProducerEpochField, Type.INT16, "Current epoch of the producer"),
    new Field(LastSequenceField, Type.INT32, "Last written sequence of the producer"),
    new Field(LastOffsetField, Type.INT64, "Last written offset of the producer"),
    new Field(OffsetDeltaField, Type.INT32, "The difference of the last sequence and first sequence in the last written batch"),
    new Field(TimestampField, Type.INT64, "Max timestamp from the last written entry"),
    new Field(CoordinatorEpochField, Type.INT32, "The epoch of the last transaction coordinator to send an end transaction marker"),
    new Field(CurrentTxnFirstOffsetField, Type.INT64, "The first offset of the on-going transaction (-1 if there is none)"))
  //生产者快照文件的格式
  val PidSnapshotMapSchema = new Schema(
    new Field(VersionField, Type.INT16, "Version of the snapshot file"),
    new Field(CrcField, Type.UNSIGNED_INT32, "CRC of the snapshot data"),
    new Field(ProducerEntriesField, new ArrayOf(ProducerSnapshotEntrySchema), "The entries in the producer table"))

  def readSnapshot(file: File): Iterable[ProducerStateEntry] = {
    try {
      val buffer = Files.readAllBytes(file.toPath)
      val struct = PidSnapshotMapSchema.read(ByteBuffer.wrap(buffer))

      val version = struct.getShort(VersionField)
      if (version != ProducerSnapshotVersion)
        throw new CorruptSnapshotException(s"Snapshot contained an unknown file version $version")

      val crc = struct.getUnsignedInt(CrcField)
      val computedCrc =  Crc32C.compute(buffer, ProducerEntriesOffset, buffer.length - ProducerEntriesOffset)
      if (crc != computedCrc)
        throw new CorruptSnapshotException(s"Snapshot is corrupt (CRC is no longer valid). " +
          s"Stored crc: $crc. Computed crc: $computedCrc")

      struct.getArray(ProducerEntriesField).map { producerEntryObj =>
        val producerEntryStruct = producerEntryObj.asInstanceOf[Struct]
        val producerId: Long = producerEntryStruct.getLong(ProducerIdField)
        val producerEpoch = producerEntryStruct.getShort(ProducerEpochField)
        val seq = producerEntryStruct.getInt(LastSequenceField)
        val offset = producerEntryStruct.getLong(LastOffsetField)
        val timestamp = producerEntryStruct.getLong(TimestampField)
        val offsetDelta = producerEntryStruct.getInt(OffsetDeltaField)
        val coordinatorEpoch = producerEntryStruct.getInt(CoordinatorEpochField)
        val currentTxnFirstOffset = producerEntryStruct.getLong(CurrentTxnFirstOffsetField)
        val newEntry = new ProducerStateEntry(producerId, mutable.Queue[BatchMetadata](BatchMetadata(seq, offset, offsetDelta, timestamp)), producerEpoch,
          coordinatorEpoch, if (currentTxnFirstOffset >= 0) Some(currentTxnFirstOffset) else None)
        newEntry
      }
    } catch {
      case e: SchemaException =>
        throw new CorruptSnapshotException(s"Snapshot failed schema validation: ${e.getMessage}")
    }
  }

  /**
   * 写入快照文件
   * @param file: 快照文件
   * @param entries: 生产者信息
   */
  private def writeSnapshot(file: File, entries: mutable.Map[Long, ProducerStateEntry]) {
    //根据结构创建一个序列化struct
    val struct = new Struct(PidSnapshotMapSchema)

    //设置快照版本
    struct.set(VersionField, ProducerSnapshotVersion)

    //CRC校验和, 后面会覆盖这里的值
    struct.set(CrcField, 0L) // we'll fill this after writing the entries

    val entriesArray = entries.map {
      case (producerId, entry) =>
        val producerEntryStruct = struct.instance(ProducerEntriesField)
        //设置每个生产者信息
        producerEntryStruct.set(ProducerIdField, producerId)
          .set(ProducerEpochField, entry.producerEpoch)
          .set(LastSequenceField, entry.lastSeq)
          .set(LastOffsetField, entry.lastDataOffset)
          .set(OffsetDeltaField, entry.lastOffsetDelta)
          .set(TimestampField, entry.lastTimestamp)
          .set(CoordinatorEpochField, entry.coordinatorEpoch)
          .set(CurrentTxnFirstOffsetField, entry.currentTxnFirstOffset.getOrElse(-1L))
        producerEntryStruct
    }.toArray
    //保存所有的生产者信息
    struct.set(ProducerEntriesField, entriesArray)

    //序列化
    val buffer = ByteBuffer.allocate(struct.sizeOf)
    struct.writeTo(buffer)
    buffer.flip()

    // now fill in the CRC
    // 填充CRC校验和
    val crc = Crc32C.compute(buffer, ProducerEntriesOffset, buffer.limit() - ProducerEntriesOffset)
    ByteUtils.writeUnsignedInt(buffer, CrcOffset, crc)

    //写入文件
    val fos = new FileOutputStream(file)
    try {
      fos.write(buffer.array, buffer.arrayOffset, buffer.limit())
    } finally {
      fos.close()
    }
  }

  private def isSnapshotFile(file: File): Boolean = file.getName.endsWith(Log.ProducerSnapshotFileSuffix)

  // visible for testing
  /**
   * 获取目录dir下的所有快照文件(以.snapshot结尾)
   * @param dir
   * @return
   */
  private[log] def listSnapshotFiles(dir: File): Seq[File] = {
    if (dir.exists && dir.isDirectory) {
      Option(dir.listFiles).map { files =>
        files.filter(f => f.isFile && isSnapshotFile(f)).toSeq
      }.getOrElse(Seq.empty)
    } else Seq.empty
  }

  // visible for testing
  //删除目录dir下所有位移比参数offset小的快照文件
  private[log] def deleteSnapshotsBefore(dir: File, offset: Long): Unit = deleteSnapshotFiles(dir, _ < offset)

  private def deleteSnapshotFiles(dir: File, predicate: Long => Boolean = _ => true) {
    listSnapshotFiles(dir).filter(file => predicate(offsetFromFile(file))).foreach { file =>
      Files.deleteIfExists(file.toPath)
    }
  }

}

/**
 * Maintains a mapping from ProducerIds to metadata about the last appended entries (e.g.
 * epoch, sequence number, last offset, etc.)
 *
 * The sequence number is the last number successfully appended to the partition for the given identifier.
 * The epoch is used for fencing against zombie writers. The offset is the one of the last successful message
 * appended to the partition.
 *
 * As long as a producer id is contained in the map, the corresponding producer can continue to write data.
 * However, producer ids can be expired due to lack of recent use or if the last written entry has been deleted from
 * the log (e.g. if the retention policy is "delete"). For compacted topics, the log cleaner will ensure
 * that the most recent entry from a given producer id is retained in the log provided it hasn't expired due to
 * age. This ensures that producer ids will not be expired until either the max expiration time has been reached,
 * or if the topic also is configured for deletion, the segment containing the last written offset has
 * been deleted.
 *
 * 保存生产者ID与其最后追加消息元数据的映射, 元数据例如epoch, 序列号, 最后位移等等.
 *
 * 序列号为追加到分区的最后标识符数字.
 * epoch用来防止僵尸生产者.
 * 位移为最后追加的消息位移.
 *
 * 只要一个生产者ID存在此map中, 其对应的生产者可以写入数据. 如果生产者超过一定时间不活跃或者最后写入的条目已经被删除(例如由于日志保留
 * 策略为"deleted"), 那么生产者ID会过期. 对于compact格式的主题, 日志cleaner会保证对于生产者ID保留的是最新的未过期条目. 这保证了
 * 只有到达了最大超时时间,或者日志保留策略设置为"deleted"并且包含最后写入位移的日志段已经被删除, 生产者ID才会过期.
 */
@nonthreadsafe
class ProducerStateManager(val topicPartition: TopicPartition,
                           @volatile var logDir: File,
                           val maxProducerIdExpirationMs: Int = 60 * 60 * 1000) extends Logging {
  import ProducerStateManager._
  import java.util

  this.logIdent = s"[ProducerStateManager partition=$topicPartition] "

  private val producers = mutable.Map.empty[Long, ProducerStateEntry]
  private var lastMapOffset = 0L
  private var lastSnapOffset = 0L

  // ongoing transactions sorted by the first offset of the transaction
  private val ongoingTxns = new util.TreeMap[Long, TxnMetadata]

  // completed transactions whose markers are at offsets above the high watermark
  private val unreplicatedTxns = new util.TreeMap[Long, TxnMetadata]

  /**
   * An unstable offset is one which is either undecided (i.e. its ultimate outcome is not yet known),
   * or one that is decided, but may not have been replicated (i.e. any transaction which has a COMMIT/ABORT
   * marker written at a higher offset than the current high watermark).
   *
   * 获取第一条unstable的位移, 取"未复制事务消息"和"未完成事务消息"中的位移最小者
   */
  def firstUnstableOffset: Option[LogOffsetMetadata] = {
    //获取第一条未复制的事务消息位移
    val unreplicatedFirstOffset = Option(unreplicatedTxns.firstEntry).map(_.getValue.firstOffset)
    //获取第一条正在进行中的事务消息位移
    val undecidedFirstOffset = Option(ongoingTxns.firstEntry).map(_.getValue.firstOffset)

    //以下代码获取上述位移的最小值(需要判空)
    if (unreplicatedFirstOffset.isEmpty)
      undecidedFirstOffset
    else if (undecidedFirstOffset.isEmpty)
      unreplicatedFirstOffset
    else if (undecidedFirstOffset.get.messageOffset < unreplicatedFirstOffset.get.messageOffset)
      undecidedFirstOffset
    else
      unreplicatedFirstOffset
  }

  /**
   * Acknowledge all transactions which have been completed before a given offset. This allows the LSO
   * to advance to the next unstable offset.
   *
   * 根据highWatermark, 确认它之前的事务已经复制. 这样后续的代码可以更新最新的stable位移(Last Stable Offset).
   */
  def onHighWatermarkUpdated(highWatermark: Long): Unit = {
    removeUnreplicatedTransactions(highWatermark)
  }

  /**
   * The first undecided offset is the earliest transactional message which has not yet been committed
   * or aborted.
   */
  def firstUndecidedOffset: Option[Long] = Option(ongoingTxns.firstEntry).map(_.getValue.firstOffset.messageOffset)

  /**
   * Returns the last offset of this map
   * 返回此map的最新位移
   */
  def mapEndOffset = lastMapOffset

  /**
   * Get a copy of the active producers
   */
  def activeProducers: immutable.Map[Long, ProducerStateEntry] = producers.toMap

  def isEmpty: Boolean = producers.isEmpty && unreplicatedTxns.isEmpty

  private def loadFromSnapshot(logStartOffset: Long, currentTime: Long) {
    while (true) {
      latestSnapshotFile match {
        case Some(file) =>
          try {
            info(s"Loading producer state from snapshot file '$file'")
            val loadedProducers = readSnapshot(file).filter { producerEntry =>
              isProducerRetained(producerEntry, logStartOffset) && !isProducerExpired(currentTime, producerEntry)
            }
            loadedProducers.foreach(loadProducerEntry)
            lastSnapOffset = offsetFromFile(file)
            lastMapOffset = lastSnapOffset
            return
          } catch {
            case e: CorruptSnapshotException =>
              warn(s"Failed to load producer snapshot from '$file': ${e.getMessage}")
              Files.deleteIfExists(file.toPath)
          }
        case None =>
          lastSnapOffset = logStartOffset
          lastMapOffset = logStartOffset
          return
      }
    }
  }

  // visible for testing
  private[log] def loadProducerEntry(entry: ProducerStateEntry): Unit = {
    val producerId = entry.producerId
    producers.put(producerId, entry)
    entry.currentTxnFirstOffset.foreach { offset =>
      ongoingTxns.put(offset, new TxnMetadata(producerId, offset))
    }
  }

  private def isProducerExpired(currentTimeMs: Long, producerState: ProducerStateEntry): Boolean =
    producerState.currentTxnFirstOffset.isEmpty && currentTimeMs - producerState.lastTimestamp >= maxProducerIdExpirationMs

  /**
   * Expire any producer ids which have been idle longer than the configured maximum expiration timeout.
   *
   * 将空闲时间超过最大超时时间的生产者移除
   */
  def removeExpiredProducers(currentTimeMs: Long) {
    producers.retain { case (_, lastEntry) =>
      !isProducerExpired(currentTimeMs, lastEntry)
    }
  }

  /**
   * Truncate the producer id mapping to the given offset range and reload the entries from the most recent
   * snapshot in range (if there is one). Note that the log end offset is assumed to be less than
   * or equal to the high watermark.
   */
  //只保留最后写入消息位移为(logStartOffset, logEndOffset]之间的生产者信息
  def truncateAndReload(logStartOffset: Long, logEndOffset: Long, currentTimeMs: Long) {
    // remove all out of range snapshots
    //删除不在(logStartOffset, logEndOffset]之间的生产者快照文件
    deleteSnapshotFiles(logDir, { snapOffset =>
      snapOffset > logEndOffset || snapOffset <= logStartOffset
    })

    if (logEndOffset != mapEndOffset) {
      //重新从快照文件中恢复生产者信息
      producers.clear()
      ongoingTxns.clear()

      // since we assume that the offset is less than or equal to the high watermark, it is
      // safe to clear the unreplicated transactions
      unreplicatedTxns.clear()
      loadFromSnapshot(logStartOffset, currentTimeMs)
    } else {
      //删除老的生产者信息
      truncateHead(logStartOffset)
    }
  }

  def prepareUpdate(producerId: Long, isFromClient: Boolean): ProducerAppendInfo = {
    val validationToPerform =
      if (!isFromClient)
        ValidationType.None
      else if (topicPartition.topic == Topic.GROUP_METADATA_TOPIC_NAME)
        ValidationType.EpochOnly
      else
        ValidationType.Full

    val currentEntry = lastEntry(producerId).getOrElse(ProducerStateEntry.empty(producerId))
    new ProducerAppendInfo(producerId, currentEntry, validationToPerform)
  }

  /**
   * Update the mapping with the given append information
   */
  def update(appendInfo: ProducerAppendInfo): Unit = {
    if (appendInfo.producerId == RecordBatch.NO_PRODUCER_ID)
      throw new IllegalArgumentException(s"Invalid producer id ${appendInfo.producerId} passed to update " +
        s"for partition $topicPartition")

    trace(s"Updated producer ${appendInfo.producerId} state to $appendInfo")
    val updatedEntry = appendInfo.toEntry
    producers.get(appendInfo.producerId) match {
      case Some(currentEntry) =>
        currentEntry.update(updatedEntry)

      case None =>
        producers.put(appendInfo.producerId, updatedEntry)
    }

    appendInfo.startedTransactions.foreach { txn =>
      ongoingTxns.put(txn.firstOffset.messageOffset, txn)
    }
  }

  def updateMapEndOffset(lastOffset: Long): Unit = {
    lastMapOffset = lastOffset
  }

  /**
   * Get the last written entry for the given producer id.
   * 根据生产者ID获取最后写入的信息
   */
  def lastEntry(producerId: Long): Option[ProducerStateEntry] = producers.get(producerId)

  /**
   * Take a snapshot at the current end offset if one does not already exist.
   * 在当前结束位移生成一个快照
   */
  def takeSnapshot(): Unit = {
    // If not a new offset, then it is not worth taking another snapshot
    // 只有位移比上一次快照位移之后更新了才生成快照
    if (lastMapOffset > lastSnapOffset) {
      //创建快照文件
      val snapshotFile = Log.producerSnapshotFile(logDir, lastMapOffset)

      //写入生产者快照
      info(s"Writing producer snapshot at offset $lastMapOffset")
      writeSnapshot(snapshotFile, producers)

      // Update the last snap offset according to the serialized map
      lastSnapOffset = lastMapOffset
    }
  }

  /**
   * Get the last offset (exclusive) of the latest snapshot file.
   */
  def latestSnapshotOffset: Option[Long] = latestSnapshotFile.map(file => offsetFromFile(file))

  /**
   * Get the last offset (exclusive) of the oldest snapshot file.
   */
  def oldestSnapshotOffset: Option[Long] = oldestSnapshotFile.map(file => offsetFromFile(file))

  private def isProducerRetained(producerStateEntry: ProducerStateEntry, logStartOffset: Long): Boolean = {
    producerStateEntry.removeBatchesOlderThan(logStartOffset)
    producerStateEntry.lastDataOffset >= logStartOffset
  }

  /**
   * When we remove the head of the log due to retention, we need to clean up the id map. This method takes
   * the new start offset and removes all producerIds which have a smaller last written offset. Additionally,
   * we remove snapshots older than the new log start offset.
   *
   * Note that snapshots from offsets greater than the log start offset may have producers included which
   * should no longer be retained: these producers will be removed if and when we need to load state from
   * the snapshot.
   *
   */
  //1)删除最后写入消息比logStartOffset还老的生产者(及其进行中的事务), 以及比logStartOffset还老且未完成复制的事务;
  //2)删除比logStartOffset还老的生产者快照文件
  def truncateHead(logStartOffset: Long) {
    //找出最后写入消息比logStartOffset还老的生产者列表
    val evictedProducerEntries = producers.filter { case (_, producerState) =>
      !isProducerRetained(producerState, logStartOffset)
    }
    val evictedProducerIds = evictedProducerEntries.keySet

    //删除这些失效的生产者
    producers --= evictedProducerIds

    //删除这些生产者的正在执行的事务
    removeEvictedOngoingTransactions(evictedProducerIds)

    //删除比logStartOffset还老而且仍未完成复制的事务
    removeUnreplicatedTransactions(logStartOffset)

    //校准logMapOffset
    if (lastMapOffset < logStartOffset)
      lastMapOffset = logStartOffset

    //删除比logStartOffset还老的生产者快照文件
    deleteSnapshotsBefore(logStartOffset)

    //校准lastSnapOffset
    lastSnapOffset = latestSnapshotOffset.getOrElse(logStartOffset)
  }

  private def removeEvictedOngoingTransactions(expiredProducerIds: collection.Set[Long]): Unit = {
    val iterator = ongoingTxns.entrySet.iterator
    while (iterator.hasNext) {
      val txnEntry = iterator.next()
      if (expiredProducerIds.contains(txnEntry.getValue.producerId))
        iterator.remove()
    }
  }

  /**
   * 确认offset之前的事务已经复制
   * @param offset
   */
  private def removeUnreplicatedTransactions(offset: Long): Unit = {
    val iterator = unreplicatedTxns.entrySet.iterator
    while (iterator.hasNext) {
      val txnEntry = iterator.next()
      val lastOffset = txnEntry.getValue.lastOffset
      if (lastOffset.exists(_ < offset))
        iterator.remove()
    }
  }

  /**
   * Truncate the producer id mapping and remove all snapshots. This resets the state of the mapping.
   * 截断清理生产者映射并删除所有快照文件, 重置映射状态.
   */
  def truncate() {
    //清理生产者映射
    producers.clear()
    //清理进行中的事务数据
    ongoingTxns.clear()
    //清理未复制的事务
    unreplicatedTxns.clear()
    //删除生产者快照文件
    deleteSnapshotFiles(logDir)
    //重置映射状态
    lastSnapOffset = 0L
    lastMapOffset = 0L
  }

  /**
   * Complete the transaction and return the last stable offset.
   * 完成事务并返回最新的stable位移
   */
  def completeTxn(completedTxn: CompletedTxn): Long = {
    //获取并删除onGoingTxns(正在进行中的事务)的该事务数据
    val txnMetadata = ongoingTxns.remove(completedTxn.firstOffset)
    if (txnMetadata == null)
      throw new IllegalArgumentException(s"Attempted to complete transaction $completedTxn on partition $topicPartition " +
        s"which was not started")

    //更新事务的结束位移
    txnMetadata.lastOffset = Some(completedTxn.lastOffset)
    //在已完成但未复制的事务中记录此事务
    unreplicatedTxns.put(completedTxn.firstOffset, txnMetadata)

    //获取并返回最新的stable位移
    val lastStableOffset = firstUndecidedOffset.getOrElse(completedTxn.lastOffset + 1)
    lastStableOffset
  }

  @threadsafe
  def deleteSnapshotsBefore(offset: Long): Unit = ProducerStateManager.deleteSnapshotsBefore(logDir, offset)

  private def oldestSnapshotFile: Option[File] = {
    val files = listSnapshotFiles
    if (files.nonEmpty)
      Some(files.minBy(offsetFromFile))
    else
      None
  }

  private def latestSnapshotFile: Option[File] = {
    val files = listSnapshotFiles
    if (files.nonEmpty)
      Some(files.maxBy(offsetFromFile))
    else
      None
  }

  private def listSnapshotFiles: Seq[File] = ProducerStateManager.listSnapshotFiles(logDir)

}

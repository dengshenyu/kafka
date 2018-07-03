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

import java.nio.ByteBuffer

import kafka.common.LongRef
import kafka.message.{CompressionCodec, NoCompressionCodec}
import kafka.utils.Logging
import org.apache.kafka.common.errors.{InvalidTimestampException, UnsupportedForMessageFormatException}
import org.apache.kafka.common.record.{AbstractRecords, CompressionType, InvalidRecordException, MemoryRecords, Record, RecordBatch, RecordConversionStats, TimestampType}
import org.apache.kafka.common.utils.Time

import scala.collection.mutable
import scala.collection.JavaConverters._

private[kafka] object LogValidator extends Logging {

  /**
   * Update the offsets for this message set and do further validation on messages including:
   * 1. Messages for compacted topics must have keys
   * 2. When magic value >= 1, inner messages of a compressed message set must have monotonically increasing offsets
   *    starting from 0.
   * 3. When magic value >= 1, validate and maybe overwrite timestamps of messages.
   * 4. Declared count of records in DefaultRecordBatch must match number of valid records contained therein.
   *
   * This method will convert messages as necessary to the topic's configured message format version. If no format
   * conversion or value overwriting is required for messages, this method will perform in-place operations to
   * avoid expensive re-compression.
   *
   * Returns a ValidationAndOffsetAssignResult containing the validated message set, maximum timestamp, the offset
   * of the shallow message with the max timestamp and a boolean indicating whether the message sizes may have changed.
   *
   * 更新消息集的位移, 并且作进一步的消息验证, 如下所示:
   * 1) Compaction的主题消息必须要有key;
   * 2) 当magic值>=1时, Compaction的消息集内部的消息必须要有从0开始的单调递增的位移;
   * 3) 当magic值>=1时, 验证(并且可能覆盖)消息的时间戳;
   * 4) DefaultRecordBatch中声明的记录数必须与其包含的合法记录数相匹配
   *
   * 此方法可以将消息转化成主题所配置的消息格式版本, 如果没有配置格式转化而且也不需要覆盖值, 那么会避免重复压缩.
   * 此方法返回ValidationAndOffsetAssignResult, 其包含经过验证的消息集, 最大的时间戳, 消息位移, 和消息大小是否改变的指示符
   *
   * 参数 records: 原消息记录
   * 参数 offsetCounter: 消息位移计数器
   * 参数 time: 用于获取时间
   * 参数 now: 当前时间
   * 参数 sourceCodec: 原消息格式
   * 参数 targetCodec: 目标消息格式
   * 参数 compactedTopic: 是否为紧凑的主题格式
   * 参数 magic: broker设置的消息格式
   * 参数 timestampType: 表示消息中的时间戳为"创建消息的时间"还是"追加到日志中的时间"
   * 参数 timestampDiffMaxMs: 当timestampType为"创建消息的时间"时, broker检查当前时间和消息中的时间戳差值, 最大不能超过此值
   * 参数 partitionLeaderEpoch: 消息的leader epoch
   * 参数 isFromClient: 是否来源于生产者
   */
  private[kafka] def validateMessagesAndAssignOffsets(records: MemoryRecords,
                                                      offsetCounter: LongRef,
                                                      time: Time,
                                                      now: Long,
                                                      sourceCodec: CompressionCodec,
                                                      targetCodec: CompressionCodec,
                                                      compactedTopic: Boolean,
                                                      magic: Byte,
                                                      timestampType: TimestampType,
                                                      timestampDiffMaxMs: Long,
                                                      partitionLeaderEpoch: Int,
                                                      isFromClient: Boolean): ValidationAndOffsetAssignResult = {
    if (sourceCodec == NoCompressionCodec && targetCodec == NoCompressionCodec) {
      // check the magic value
      //如果消息没有压缩, 而且源消息与broker设置的消息格式不同, 那么需要转化并且设置位移
      if (!records.hasMatchingMagic(magic))
        convertAndAssignOffsetsNonCompressed(records, offsetCounter, compactedTopic, time, now, timestampType,
          timestampDiffMaxMs, magic, partitionLeaderEpoch, isFromClient)
      else
        // Do in-place validation, offset assignment and maybe set timestamp
        // 如果消息没有压缩, 但是源消息与broker设置的消息格式相同, 那么直接设置位移
        assignOffsetsNonCompressed(records, offsetCounter, now, compactedTopic, timestampType, timestampDiffMaxMs,
          partitionLeaderEpoch, isFromClient, magic)
    } else {
      //如果源消息或目标消息压缩的情况
      validateMessagesAndAssignOffsetsCompressed(records, offsetCounter, time, now, sourceCodec, targetCodec, compactedTopic,
        magic, timestampType, timestampDiffMaxMs, partitionLeaderEpoch, isFromClient)
    }
  }

  /**
   * 校验batch是否合法, 如果来源于生产者那么校验会更严格
   * @param batch
   * @param isFromClient
   * @param toMagic
   */
  private def validateBatch(batch: RecordBatch, isFromClient: Boolean, toMagic: Byte): Unit = {
    if (isFromClient) {
      if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) {
        val countFromOffsets = batch.lastOffset - batch.baseOffset + 1
        if (countFromOffsets <= 0)
          throw new InvalidRecordException(s"Batch has an invalid offset range: [${batch.baseOffset}, ${batch.lastOffset}]")

        // v2 and above messages always have a non-null count
        val count = batch.countOrNull
        if (count <= 0)
          throw new InvalidRecordException(s"Invalid reported count for record batch: $count")

        if (countFromOffsets != batch.countOrNull)
          throw new InvalidRecordException(s"Inconsistent batch offset range [${batch.baseOffset}, ${batch.lastOffset}] " +
            s"and count of records $count")
      }

      if (batch.hasProducerId && batch.baseSequence < 0)
        throw new InvalidRecordException(s"Invalid sequence number ${batch.baseSequence} in record batch " +
          s"with producerId ${batch.producerId}")

      if (batch.isControlBatch)
        throw new InvalidRecordException("Clients are not allowed to write control records")
    }

    if (batch.isTransactional && toMagic < RecordBatch.MAGIC_VALUE_V2)
      throw new UnsupportedForMessageFormatException(s"Transactional records cannot be used with magic version $toMagic")

    if (batch.hasProducerId && toMagic < RecordBatch.MAGIC_VALUE_V2)
      throw new UnsupportedForMessageFormatException(s"Idempotent records cannot be used with magic version $toMagic")
  }

  /**
   * 校验消息记录是否合法
   * @param batch
   * @param record
   * @param now
   * @param timestampType
   * @param timestampDiffMaxMs
   * @param compactedTopic
   */
  private def validateRecord(batch: RecordBatch, record: Record, now: Long, timestampType: TimestampType,
                             timestampDiffMaxMs: Long, compactedTopic: Boolean): Unit = {
    if (!record.hasMagic(batch.magic))
      throw new InvalidRecordException(s"Log record magic does not match outer magic ${batch.magic}")

    // verify the record-level CRC only if this is one of the deep entries of a compressed message
    // set for magic v0 and v1. For non-compressed messages, there is no inner record for magic v0 and v1,
    // so we depend on the batch-level CRC check in Log.analyzeAndValidateRecords(). For magic v2 and above,
    // there is no record-level CRC to check.
    if (batch.magic <= RecordBatch.MAGIC_VALUE_V1 && batch.isCompressed)
      record.ensureValid()

    validateKey(record, compactedTopic)
    validateTimestamp(batch, record, now, timestampType, timestampDiffMaxMs)
  }

  /**
   * 如果消息没有压缩而且源消息与broker设置的消息格式不同, 那么需要转化并且设置位移
   *
   * @param records
   * @param offsetCounter
   * @param compactedTopic
   * @param time
   * @param now
   * @param timestampType
   * @param timestampDiffMaxMs
   * @param toMagicValue
   * @param partitionLeaderEpoch
   * @param isFromClient
   * @return
   */
  private def convertAndAssignOffsetsNonCompressed(records: MemoryRecords,
                                                   offsetCounter: LongRef,
                                                   compactedTopic: Boolean,
                                                   time: Time,
                                                   now: Long,
                                                   timestampType: TimestampType,
                                                   timestampDiffMaxMs: Long,
                                                   toMagicValue: Byte,
                                                   partitionLeaderEpoch: Int,
                                                   isFromClient: Boolean): ValidationAndOffsetAssignResult = {
    val startNanos = time.nanoseconds
    //获取转化后的字节数
    val sizeInBytesAfterConversion = AbstractRecords.estimateSizeInBytes(toMagicValue, offsetCounter.value,
      CompressionType.NONE, records.records)

    val (producerId, producerEpoch, sequence, isTransactional) = {
      val first = records.batches.asScala.head
      (first.producerId, first.producerEpoch, first.baseSequence, first.isTransactional)
    }

    //分配转化后的buffer
    val newBuffer = ByteBuffer.allocate(sizeInBytesAfterConversion)
    //用于转化的builder
    val builder = MemoryRecords.builder(newBuffer, toMagicValue, CompressionType.NONE, timestampType,
      offsetCounter.value, now, producerId, producerEpoch, sequence, isTransactional, partitionLeaderEpoch)

    //写入batch
    for (batch <- records.batches.asScala) {
      //校验batch
      validateBatch(batch, isFromClient, toMagicValue)

      //写入消息记录
      for (record <- batch.asScala) {
        //校验batch里的消息记录
        validateRecord(batch, record, now, timestampType, timestampDiffMaxMs, compactedTopic)
        //写入消息
        builder.appendWithOffset(offsetCounter.getAndIncrement(), record)
      }
    }

    //转换消息记录
    val convertedRecords = builder.build()

    //返回转换的信息
    val info = builder.info
    val recordConversionStats = new RecordConversionStats(builder.uncompressedBytesWritten,
      builder.numRecords, time.nanoseconds - startNanos)
    ValidationAndOffsetAssignResult(
      validatedRecords = convertedRecords,
      maxTimestamp = info.maxTimestamp,
      shallowOffsetOfMaxTimestamp = info.shallowOffsetOfMaxTimestamp,
      messageSizeMaybeChanged = true,
      recordConversionStats = recordConversionStats)
  }

  /**
   * 对于非压缩的消息记录, 设置位移, 时间戳等信息
   * @param records
   * @param offsetCounter
   * @param now
   * @param compactedTopic
   * @param timestampType
   * @param timestampDiffMaxMs
   * @param partitionLeaderEpoch
   * @param isFromClient
   * @param magic
   * @return
   */
  private def assignOffsetsNonCompressed(records: MemoryRecords,
                                         offsetCounter: LongRef,
                                         now: Long,
                                         compactedTopic: Boolean,
                                         timestampType: TimestampType,
                                         timestampDiffMaxMs: Long,
                                         partitionLeaderEpoch: Int,
                                         isFromClient: Boolean,
                                         magic: Byte): ValidationAndOffsetAssignResult = {
    var maxTimestamp = RecordBatch.NO_TIMESTAMP
    var offsetOfMaxTimestamp = -1L
    val initialOffset = offsetCounter.value

    for (batch <- records.batches.asScala) {
      //校验batch是否合法
      validateBatch(batch, isFromClient, magic)

      var maxBatchTimestamp = RecordBatch.NO_TIMESTAMP
      var offsetOfMaxBatchTimestamp = -1L

      for (record <- batch.asScala) {
        //校验单个记录
        validateRecord(batch, record, now, timestampType, timestampDiffMaxMs, compactedTopic)

        //计算batch的最大时间戳
        val offset = offsetCounter.getAndIncrement()
        if (batch.magic > RecordBatch.MAGIC_VALUE_V0 && record.timestamp > maxBatchTimestamp) {
          maxBatchTimestamp = record.timestamp
          offsetOfMaxBatchTimestamp = offset
        }
      }

      //计算所有batch的最大时间戳
      if (batch.magic > RecordBatch.MAGIC_VALUE_V0 && maxBatchTimestamp > maxTimestamp) {
        maxTimestamp = maxBatchTimestamp
        offsetOfMaxTimestamp = offsetOfMaxBatchTimestamp
      }

      //设置batch的最后的消息位移
      batch.setLastOffset(offsetCounter.value - 1)

      //设置分区leader epoch
      if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
        batch.setPartitionLeaderEpoch(partitionLeaderEpoch)

      //设置batch的最大时间戳
      if (batch.magic > RecordBatch.MAGIC_VALUE_V0) {
        if (timestampType == TimestampType.LOG_APPEND_TIME)
          batch.setMaxTimestamp(TimestampType.LOG_APPEND_TIME, now)
        else
          batch.setMaxTimestamp(timestampType, maxBatchTimestamp)
      }
    }

    //计算所有batch的最大时间戳
    if (timestampType == TimestampType.LOG_APPEND_TIME) {
      maxTimestamp = now
      if (magic >= RecordBatch.MAGIC_VALUE_V2)
        offsetOfMaxTimestamp = offsetCounter.value - 1
      else
        offsetOfMaxTimestamp = initialOffset
    }

    //返回结果
    ValidationAndOffsetAssignResult(
      validatedRecords = records,
      maxTimestamp = maxTimestamp,
      shallowOffsetOfMaxTimestamp = offsetOfMaxTimestamp,
      messageSizeMaybeChanged = false,
      recordConversionStats = RecordConversionStats.EMPTY)
  }

  /**
   * We cannot do in place assignment in one of the following situations:
   * 1. Source and target compression codec are different
   * 2. When magic value to use is 0 because offsets need to be overwritten
   * 3. When magic value to use is above 0, but some fields of inner messages need to be overwritten.
   * 4. Message format conversion is needed.
   *
   * 如果发生了如下情况, 不能直接在原来的记录中设置位移
   * 1. 原压缩格式与目标格式不同
   * 2. 目标的消息格式版本为0(因为位移需要被重写)
   * 3. 目标的消息格式版本大于0, 但消息内部的一些字段需要被重写
   * 4. 需要做消息转换
   */
  def validateMessagesAndAssignOffsetsCompressed(records: MemoryRecords,
                                                 offsetCounter: LongRef,
                                                 time: Time,
                                                 now: Long,
                                                 sourceCodec: CompressionCodec,
                                                 targetCodec: CompressionCodec,
                                                 compactedTopic: Boolean,
                                                 toMagic: Byte,
                                                 timestampType: TimestampType,
                                                 timestampDiffMaxMs: Long,
                                                 partitionLeaderEpoch: Int,
                                                 isFromClient: Boolean): ValidationAndOffsetAssignResult = {

      // 可以直接在原纪录中设置位移的情况
      var inPlaceAssignment = sourceCodec == targetCodec && toMagic > RecordBatch.MAGIC_VALUE_V0

      var maxTimestamp = RecordBatch.NO_TIMESTAMP
      val expectedInnerOffset = new LongRef(0)
      val validatedRecords = new mutable.ArrayBuffer[Record]

      var uncompressedSizeInBytes = 0

      for (batch <- records.batches.asScala) {
        //验证batch
        validateBatch(batch, isFromClient, toMagic)

        //未压缩字节数加上batch的头部大小
        uncompressedSizeInBytes += AbstractRecords.recordBatchHeaderSizeInBytes(toMagic, batch.compressionType())

        // 如果是控制的记录, 而且源格式时未压缩的, 那么直接在原记录基础上设置位移
        if (sourceCodec == NoCompressionCodec && batch.isControlBatch)
          inPlaceAssignment = true

        for (record <- batch.asScala) {
          //验证记录
          validateRecord(batch, record, now, timestampType, timestampDiffMaxMs, compactedTopic)

          // 外部压缩的记录内部不能拥有设置压缩属性的记录
          if (sourceCodec != NoCompressionCodec && record.isCompressed)
            throw new InvalidRecordException("Compressed outer record should not have an inner record with a " +
              s"compression attribute set: $record")

          //未压缩字节数加上记录的大小
          uncompressedSizeInBytes += record.sizeInBytes()

          if (batch.magic > RecordBatch.MAGIC_VALUE_V0 && toMagic > RecordBatch.MAGIC_VALUE_V0) {
            // 对于消息位移需要重写的情况, 不能在原记录中设置属性
            if (record.offset != expectedInnerOffset.getAndIncrement())
              inPlaceAssignment = false
            if (record.timestamp > maxTimestamp)
              maxTimestamp = record.timestamp
          }

          // 如果需要做消息转换, 那么不能在原记录中设置属性
          if (!record.hasMagic(toMagic))
            inPlaceAssignment = false

          validatedRecords += record
        }
      }

      if (!inPlaceAssignment) {
        //需要转换数据的情况
        val (producerId, producerEpoch, sequence, isTransactional) = {
          // note that we only reassign offsets for requests coming straight from a producer. For records with magic V2,
          // there should be exactly one RecordBatch per request, so the following is all we need to do. For Records
          // with older magic versions, there will never be a producer id, etc.
          // 我们只对来源于生产者的请求设置位移, 对于V2版本的记录, 每个请求中应该只有一个RecordBatch. 而对于更老版本的记录, 则
          // 不会有producer id这些信息
          val first = records.batches.asScala.head
          (first.producerId, first.producerEpoch, first.baseSequence, first.isTransactional)
        }
        //转换记录并设置位移
        buildRecordsAndAssignOffsets(toMagic, offsetCounter, time, timestampType, CompressionType.forId(targetCodec.codec), now,
          validatedRecords, producerId, producerEpoch, sequence, isTransactional, partitionLeaderEpoch, isFromClient,
          uncompressedSizeInBytes)
      } else {
        // we can update the batch only and write the compressed payload as is
        // 直接设置记录的位移
        val batch = records.batches.iterator.next()
        val lastOffset = offsetCounter.addAndGet(validatedRecords.size) - 1
        batch.setLastOffset(lastOffset)

        //设置最大时间戳
        if (timestampType == TimestampType.LOG_APPEND_TIME)
          maxTimestamp = now

        if (toMagic >= RecordBatch.MAGIC_VALUE_V1)
          batch.setMaxTimestamp(timestampType, maxTimestamp)

        //设置分区leader epoch
        if (toMagic >= RecordBatch.MAGIC_VALUE_V2)
          batch.setPartitionLeaderEpoch(partitionLeaderEpoch)

        val recordConversionStats = new RecordConversionStats(uncompressedSizeInBytes, 0, 0)
        //返回结果
        ValidationAndOffsetAssignResult(validatedRecords = records,
          maxTimestamp = maxTimestamp,
          shallowOffsetOfMaxTimestamp = lastOffset,
          messageSizeMaybeChanged = false,
          recordConversionStats = recordConversionStats)
      }
  }

  private def buildRecordsAndAssignOffsets(magic: Byte,
                                           offsetCounter: LongRef,
                                           time: Time,
                                           timestampType: TimestampType,
                                           compressionType: CompressionType,
                                           logAppendTime: Long,
                                           validatedRecords: Seq[Record],
                                           producerId: Long,
                                           producerEpoch: Short,
                                           baseSequence: Int,
                                           isTransactional: Boolean,
                                           partitionLeaderEpoch: Int,
                                           isFromClient: Boolean,
                                           uncompresssedSizeInBytes: Int): ValidationAndOffsetAssignResult = {
    val startNanos = time.nanoseconds
    //预测所需要的转换缓冲区
    val estimatedSize = AbstractRecords.estimateSizeInBytes(magic, offsetCounter.value, compressionType,
      validatedRecords.asJava)
    //生成缓冲区
    val buffer = ByteBuffer.allocate(estimatedSize)
    //生成用于转换的MemoryRecordsBuilder
    val builder = MemoryRecords.builder(buffer, magic, compressionType, timestampType, offsetCounter.value,
      logAppendTime, producerId, producerEpoch, baseSequence, isTransactional, partitionLeaderEpoch)

    validatedRecords.foreach { record =>
      //添加记录
      builder.appendWithOffset(offsetCounter.getAndIncrement(), record)
    }

    //转换
    val records = builder.build()

    val info = builder.info

    // This is not strictly correct, it represents the number of records where in-place assignment is not possible
    // instead of the number of records that were converted. It will over-count cases where the source and target are
    // message format V0 or if the inner offsets are not consecutive. This is OK since the impact is the same: we have
    // to rebuild the records (including recompression if enabled).
    // 这个数字并不准确, 它只是表示不能直接设置位移情况的记录个数, 而不是转换的记录个数. 对于原格式和目标格式都是V0的情况或者内部位移
    // 并不连续的情况, 会出现重复统计. 不过这没问题, 因为我们都需要去重建记录(包括重压缩)
    val conversionCount = builder.numRecords
    val recordConversionStats = new RecordConversionStats(uncompresssedSizeInBytes + builder.uncompressedBytesWritten,
      conversionCount, time.nanoseconds - startNanos)

    //返回结果
    ValidationAndOffsetAssignResult(
      validatedRecords = records,
      maxTimestamp = info.maxTimestamp,
      shallowOffsetOfMaxTimestamp = info.shallowOffsetOfMaxTimestamp,
      messageSizeMaybeChanged = true,
      recordConversionStats = recordConversionStats)
  }

  private def validateKey(record: Record, compactedTopic: Boolean) {
    if (compactedTopic && !record.hasKey)
      throw new InvalidRecordException("Compacted topic cannot accept message without key.")
  }

  /**
   * This method validates the timestamps of a message.
   * If the message is using create time, this method checks if it is within acceptable range.
   *
   * 校验消息的时间戳, 如果使用创建时间作为时间戳, 那么校验与当前时间的差值是否小于特定阈值(避免broker收到太久前的消息)
   */
  private def validateTimestamp(batch: RecordBatch,
                                record: Record,
                                now: Long,
                                timestampType: TimestampType,
                                timestampDiffMaxMs: Long) {
    if (timestampType == TimestampType.CREATE_TIME
      && record.timestamp != RecordBatch.NO_TIMESTAMP
      && math.abs(record.timestamp - now) > timestampDiffMaxMs)
      throw new InvalidTimestampException(s"Timestamp ${record.timestamp} of message with offset ${record.offset} is " +
        s"out of range. The timestamp should be within [${now - timestampDiffMaxMs}, ${now + timestampDiffMaxMs}]")
    if (batch.timestampType == TimestampType.LOG_APPEND_TIME)
      throw new InvalidTimestampException(s"Invalid timestamp type in message $record. Producer should not set " +
        s"timestamp type to LogAppendTime.")
  }

  case class ValidationAndOffsetAssignResult(validatedRecords: MemoryRecords,
                                             maxTimestamp: Long,
                                             shallowOffsetOfMaxTimestamp: Long,
                                             messageSizeMaybeChanged: Boolean,
                                             recordConversionStats: RecordConversionStats)

}

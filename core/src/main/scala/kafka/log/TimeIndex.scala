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
import java.nio.ByteBuffer

import kafka.utils.CoreUtils._
import kafka.utils.Logging
import org.apache.kafka.common.errors.InvalidOffsetException
import org.apache.kafka.common.record.RecordBatch

/**
 * An index that maps from the timestamp to the logical offsets of the messages in a segment. This index might be
 * sparse, i.e. it may not hold an entry for all the messages in the segment.
 *
 * The index is stored in a file that is preallocated to hold a fixed maximum amount of 12-byte time index entries.
 * The file format is a series of time index entries. The physical format is a 8 bytes timestamp and a 4 bytes "relative"
 * offset used in the [[OffsetIndex]]. A time index entry (TIMESTAMP, OFFSET) means that the biggest timestamp seen
 * before OFFSET is TIMESTAMP. i.e. Any message whose timestamp is greater than TIMESTAMP must come after OFFSET.
 *
 * All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal
 * storage format.
 *
 * The timestamps in the same time index file are guaranteed to be monotonically increasing.
 *
 * The index support timestamp lookup for a memory map of this file. The lookup is done using a binary search to find
 * the offset of the message whose indexed timestamp is closest but smaller or equals to the target timestamp.
 *
 * Time index files can be opened in two ways: either as an empty, mutable index that allows appends or
 * an immutable read-only index file that has previously been populated. The makeReadOnly method will turn a mutable file into an
 * immutable one and truncate off any extra bytes. This is done when the index file is rolled over.
 *
 * No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 *
 * 一个将时间戳映射为消息位移的索引. 这个索引可能是稀疏的, 也就是说它可能不会包含所有消息的索引.
 *
 * 此索引存储在一个文件中, 该文件预分配了能够包含最大数量的12字节索引条目空间. 文件包含一列时间戳索引条目, 每个条目为8字节的时间戳
 * 和4字节的相对位移, 该相对位移在OffsetIndex这个索引中定义. 一个时间索引条目(TIMESTAMP, OFFSET)意味着位移小于等于OFFSET的最大时间戳
 * 为TIMESTAMP, 也就是说任何时间戳大于TIMESTAMP的消息位移肯定大于OFFSET.
 *
 * 所有暴露的API已经将相对位移转换成消息位移, 因此此类内部存储格式对使用者透明.
 *
 * 一个时间戳索引内部的索引保证是单调递增的.
 *
 * 此索引使用一个内存映射区域来支持查询, 场景为查询时间戳小于等于目标时间戳的最大时间戳消息位移, 查询时使用二分搜索.
 *
 * 此索引文件有两种打开方式: 1)以空文件并且可修改的方式打开, 允许不断追加条目; 2)以不可修改的只读方式打开, 这种文件之前已经填充完毕.
 * makeReadOnly方法可以将一个可修改的文件变成一个不可修改的文件, 并且截断任何冗余字节, 此方法在索引文件滚动时使用.
 *
 * 此文件没有做内容校验, 如果损坏了则进行重建.
 */
// Avoid shadowing mutable file in AbstractIndex
class TimeIndex(_file: File, baseOffset: Long, maxIndexSize: Int = -1, writable: Boolean = true)
    extends AbstractIndex[Long, Long](_file, baseOffset, maxIndexSize, writable) with Logging {

  @volatile private var _lastEntry = lastEntryFromIndexFile

  override def entrySize = 12

  // We override the full check to reserve the last time index entry slot for the on roll call.
  override def isFull: Boolean = entries >= maxEntries - 1

  private def timestamp(buffer: ByteBuffer, n: Int): Long = buffer.getLong(n * entrySize)

  private def relativeOffset(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * entrySize + 8)

  def lastEntry: TimestampOffset = _lastEntry

  /**
   * Read the last entry from the index file. This operation involves disk access.
   */
  private def lastEntryFromIndexFile: TimestampOffset = {
    inLock(lock) {
      _entries match {
        case 0 => TimestampOffset(RecordBatch.NO_TIMESTAMP, baseOffset)
        case s => parseEntry(mmap, s - 1).asInstanceOf[TimestampOffset]
      }
    }
  }

  /**
   * Get the nth timestamp mapping from the time index
   * @param n The entry number in the time index
   * @return The timestamp/offset pair at that entry
   */
  def entry(n: Int): TimestampOffset = {
    maybeLock(lock) {
      if(n >= _entries)
        throw new IllegalArgumentException("Attempt to fetch the %dth entry from a time index of size %d.".format(n, _entries))
      val idx = mmap.duplicate
      TimestampOffset(timestamp(idx, n), relativeOffset(idx, n))
    }
  }

  override def parseEntry(buffer: ByteBuffer, n: Int): IndexEntry = {
    TimestampOffset(timestamp(buffer, n), baseOffset + relativeOffset(buffer, n))
  }

  /**
   * Attempt to append a time index entry to the time index.
   * The new entry is appended only if both the timestamp and offsets are greater than the last appended timestamp and
   * the last appended offset.
   *
   * @param timestamp The timestamp of the new time index entry
   * @param offset The offset of the new time index entry
   * @param skipFullCheck To skip checking whether the segment is full or not. We only skip the check when the segment
   *                      gets rolled or the segment is closed.
   *
   * 舱室在日志索引中增加一个时间索引条目. 当且仅当新的时间戳和索引都比文件中最后的时间戳和索引都大, 才会新增此条目.
   * 参数 时间戳: 新索引条目的时间戳
   * 参数 位移: 新索引条目的位移
   * 参数 skipFullCheck: 是否跳过检查索引文件是否已经填满. 只有在日志段滚动或者关闭的情况下才会跳过此检查.
   */
  def maybeAppend(timestamp: Long, offset: Long, skipFullCheck: Boolean = false) {
    inLock(lock) {
      if (!skipFullCheck)
        require(!isFull, "Attempt to append to a full time index (size = " + _entries + ").")
      // We do not throw exception when the offset equals to the offset of last entry. That means we are trying
      // to insert the same time index entry as the last entry.
      // If the timestamp index entry to be inserted is the same as the last entry, we simply ignore the insertion
      // because that could happen in the following two scenarios:
      // 1. A log segment is closed.
      // 2. LogSegment.onBecomeInactiveSegment() is called when an active log segment is rolled.
      //
      // 如果新条目的位移或时间戳比最后的条目小, 那么抛出异常.
      // 如果新条目和最后的条目完全一样, 那么会忽略插入但不抛出异常, 这种情况会发生在如下场景:
      // 1. 日志段关闭
      // 2. 日志段滚动, 调用LogSegment.onBecomeInactiveSegment()
      if (_entries != 0 && offset < lastEntry.offset)
        throw new InvalidOffsetException("Attempt to append an offset (%d) to slot %d no larger than the last offset appended (%d) to %s."
          .format(offset, _entries, lastEntry.offset, file.getAbsolutePath))
      if (_entries != 0 && timestamp < lastEntry.timestamp)
        throw new IllegalStateException("Attempt to append a timestamp (%d) to slot %d no larger than the last timestamp appended (%d) to %s."
            .format(timestamp, _entries, lastEntry.timestamp, file.getAbsolutePath))
      // We only append to the time index when the timestamp is greater than the last inserted timestamp.
      // If all the messages are in message format v0, the timestamp will always be NoTimestamp. In that case, the time
      // index will be empty.
      //
      // 当新条目的时间戳比最后的时间戳大, 才新增条目.
      if (timestamp > lastEntry.timestamp) {
        debug("Adding index entry %d => %d to %s.".format(timestamp, offset, file.getName))
        //记录时间戳
        mmap.putLong(timestamp)
        //记录相对位移
        mmap.putInt(relativeOffset(offset))

        _entries += 1
        _lastEntry = TimestampOffset(timestamp, offset)
        require(_entries * entrySize == mmap.position(), _entries + " entries but file position in index is " + mmap.position() + ".")
      }
    }
  }

  /**
   * Find the time index entry whose timestamp is less than or equal to the given timestamp.
   * If the target timestamp is smaller than the least timestamp in the time index, (NoTimestamp, baseOffset) is
   * returned.
   *
   * @param targetTimestamp The timestamp to look up.
   * @return The time index entry found.
   *
   * 查询时间戳小于等于指定时间戳的索引条目. 如果目标时间戳比时间索引的最小时间戳小, 那么返回(NoTimestamp, baseOffset)
   *
   * 参数 targetTimestamp: 目标时间戳
   * 返回 找到的时间索引条目
   */
  def lookup(targetTimestamp: Long): TimestampOffset = {
    maybeLock(lock) {
      val idx = mmap.duplicate
      //查找小于等于目标时间戳的最大条目
      val slot = largestLowerBoundSlotFor(idx, targetTimestamp, IndexSearchType.KEY)

      //返回结果
      if (slot == -1)
        TimestampOffset(RecordBatch.NO_TIMESTAMP, baseOffset)
      else {
        val entry = parseEntry(idx, slot).asInstanceOf[TimestampOffset]
        TimestampOffset(entry.timestamp, entry.offset)
      }
    }
  }

  override def truncate() = truncateToEntries(0)

  /**
   * Remove all entries from the index which have an offset greater than or equal to the given offset.
   * Truncating to an offset larger than the largest in the index has no effect.
   */
  override def truncateTo(offset: Long) {
    inLock(lock) {
      val idx = mmap.duplicate
      val slot = largestLowerBoundSlotFor(idx, offset, IndexSearchType.VALUE)

      /* There are 3 cases for choosing the new size
       * 1) if there is no entry in the index <= the offset, delete everything
       * 2) if there is an entry for this exact offset, delete it and everything larger than it
       * 3) if there is no entry for this offset, delete everything larger than the next smallest
       */
      val newEntries =
        if(slot < 0)
          0
        else if(relativeOffset(idx, slot) == offset - baseOffset)
          slot
        else
          slot + 1
      truncateToEntries(newEntries)
    }
  }

  override def resize(newSize: Int): Boolean = {
    inLock(lock) {
      if (super.resize(newSize)) {
        _lastEntry = lastEntryFromIndexFile
        true
      } else
        false
    }
  }

  /**
   * Truncates index to a known number of entries.
   */
  private def truncateToEntries(entries: Int) {
    inLock(lock) {
      _entries = entries
      mmap.position(_entries * entrySize)
      _lastEntry = lastEntryFromIndexFile
    }
  }

  override def sanityCheck() {
    val lastTimestamp = lastEntry.timestamp
    val lastOffset = lastEntry.offset
    if (_entries != 0 && lastTimestamp < timestamp(mmap, 0))
      throw new CorruptIndexException(s"Corrupt time index found, time index file (${file.getAbsolutePath}) has " +
        s"non-zero size but the last timestamp is $lastTimestamp which is less than the first timestamp " +
        s"${timestamp(mmap, 0)}")
    if (_entries != 0 && lastOffset < baseOffset)
      throw new CorruptIndexException(s"Corrupt time index found, time index file (${file.getAbsolutePath}) has " +
        s"non-zero size but the last offset is $lastOffset which is less than the first offset $baseOffset")
    if (length % entrySize != 0)
      throw new CorruptIndexException(s"Time index file ${file.getAbsolutePath} is corrupt, found $length bytes " +
        s"which is neither positive nor a multiple of $entrySize.")
  }
}

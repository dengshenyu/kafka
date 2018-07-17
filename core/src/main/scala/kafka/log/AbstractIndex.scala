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

import java.io.{File, RandomAccessFile}
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.util.concurrent.locks.{Lock, ReentrantLock}

import kafka.common.IndexOffsetOverflowException
import kafka.log.IndexSearchType.IndexSearchEntity
import kafka.utils.CoreUtils.inLock
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.utils.{MappedByteBuffers, OperatingSystem, Utils}

import scala.math.ceil

/**
 * The abstract index class which holds entry format agnostic methods.
 *
 * @param file The index file
 * @param baseOffset the base offset of the segment that this index is corresponding to.
 * @param maxIndexSize The maximum index size in bytes.
 *
 * 保存索引条目的泛型抽象类
 *
 * 参数 file: 索引文件
 * 参数 baseOffset: 此索引对应日志段的基准位移
 * 参数 maxIndexSize: 索引最大字节数
 * 参数 writable: 是否可写
 */
abstract class AbstractIndex[K, V](@volatile var file: File, val baseOffset: Long,
                                   val maxIndexSize: Int = -1, val writable: Boolean) extends Logging {

  // Length of the index file
  @volatile
  private var _length: Long = _

  protected def entrySize: Int

  protected val lock = new ReentrantLock

  /**
   * 创建一个内存映射缓冲区
   */
  @volatile
  protected var mmap: MappedByteBuffer = {
    //创建并打开文件
    val newlyCreated = file.createNewFile()
    val raf = if (writable) new RandomAccessFile(file, "rw") else new RandomAccessFile(file, "r")

    try {
      /* pre-allocate the file if necessary */
      //预分配文件空间, 使其根据条目大小计算出小于等于maxIndexSize的最大值
      if(newlyCreated) {
        if(maxIndexSize < entrySize)
          throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize)
        raf.setLength(roundDownToExactMultiple(maxIndexSize, entrySize))
      }

      /* memory-map the file */
      //将整个文件映射到内存
      _length = raf.length()
      val idx = {
        if (writable)
          raf.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, _length)
        else
          raf.getChannel.map(FileChannel.MapMode.READ_ONLY, 0, _length)
      }

      /* set the position in the index for the next entry */
      // 设置下一个索引条目的位置, 如果为新索引文件则为0, 否则指向最后的文件结尾
      if(newlyCreated)
        idx.position(0)
      else
        // if this is a pre-existing index, assume it is valid and set position to last entry
        idx.position(roundDownToExactMultiple(idx.limit(), entrySize))
      idx
    } finally {
      CoreUtils.swallow(raf.close(), this)
    }
  }

  /**
   * The maximum number of entries this index can hold
   * 此索引能够包含的最大索引条目数量
   */
  @volatile
  private[this] var _maxEntries = mmap.limit() / entrySize

  /** The number of entries in this index */
  //此索引目前已包含的条目数量
  @volatile
  protected var _entries = mmap.position() / entrySize

  /**
   * True iff there are no more slots available in this index
   * 当前仅当此索引不能包含更多的索引槽时为true
   */
  def isFull: Boolean = _entries >= _maxEntries

  def maxEntries: Int = _maxEntries

  def entries: Int = _entries

  def length: Long = _length

  /**
   * Reset the size of the memory map and the underneath file. This is used in two kinds of cases: (1) in
   * trimToValidSize() which is called at closing the segment or new segment being rolled; (2) at
   * loading segments from disk or truncating back to an old segment where a new log segment became active;
   * we want to reset the index size to maximum index size to avoid rolling new segment.
   *
   * @param newSize new size of the index file
   * @return a boolean indicating whether the size of the memory map and the underneath file is changed or not.
   *
   * 重置内存映射空间和其底下文件的大小, 此方法主要在两种场景下使用:
   * 1) 在关闭日志段或者滚动日志时, 在trimToValidSize()方法中调用;
   * 2) 在新的日志段即将使用时, 加载日志段或者把老日志段截断;
   * 将索引大小重置成最大可以避免滚动日志
   *
   * 参数 newSize 新索引文件的大小
   * 返回 内存映射空间或底下文件是否被更改
   */
  def resize(newSize: Int): Boolean = {
    inLock(lock) {
      //每个条目大小为entrySize, 返回小于或等于newSize且最接近它的entrySize倍数
      val roundedNewSize = roundDownToExactMultiple(newSize, entrySize)

      if (_length == roundedNewSize) {
        //如果索引长度刚好等于新的长度, 那么不用更改
        false
      } else {

        val raf = new RandomAccessFile(file, "rw")
        try {
          val position = mmap.position()

          /* Windows won't let us modify the file length while the file is mmapped :-( */
          // Windows系统不能更改mmaped文件的大小, 这里先做unmap
          if (OperatingSystem.IS_WINDOWS)
            safeForceUnmap()
          //设置索引文件为新的大小
          raf.setLength(roundedNewSize)
          _length = roundedNewSize
          //重新mmap
          mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, roundedNewSize)
          _maxEntries = mmap.limit() / entrySize
          mmap.position(position)
          true
        } finally {
          CoreUtils.swallow(raf.close(), this)
        }
      }
    }
  }

  /**
   * Rename the file that backs this offset index
   *
   * @throws IOException if rename fails
   */
  def renameTo(f: File) {
    try Utils.atomicMoveWithFallback(file.toPath, f.toPath)
    finally file = f
  }

  /**
   * Flush the data in the index to disk
   */
  def flush() {
    inLock(lock) {
      mmap.force()
    }
  }

  /**
   * Delete this index file.
   *
   * @throws IOException if deletion fails due to an I/O error
   * @return `true` if the file was deleted by this method; `false` if the file could not be deleted because it did
   *         not exist
   * 删除此索引文件
   */
  def deleteIfExists(): Boolean = {
    inLock(lock) {
      // On JVM, a memory mapping is typically unmapped by garbage collector.
      // However, in some cases it can pause application threads(STW) for a long moment reading metadata from a physical disk.
      // To prevent this, we forcefully cleanup memory mapping within proper execution which never affects API responsiveness.
      // See https://issues.apache.org/jira/browse/KAFKA-4614 for the details.
      // 在JVM中, 一个内存映射区域通常由gc收集器来做unmapped操作, 但在某些场景下可能会导致线程由于需要从磁盘上读取元数据而长时间停顿.
      // 为了避免这种情况, 这里强制清理内存映射, 内部实现比较严谨, 不会导致API失去响应.
      safeForceUnmap()
    }
    Files.deleteIfExists(file.toPath)
  }

  /**
   * Trim this segment to fit just the valid entries, deleting all trailing unwritten bytes from
   * the file.
   */
  def trimToValidSize() {
    inLock(lock) {
      resize(entrySize * _entries)
    }
  }

  /**
   * The number of bytes actually used by this index
   */
  def sizeInBytes = entrySize * _entries

  /** Close the index */
  def close() {
    trimToValidSize()
  }

  def closeHandler(): Unit = {
    inLock(lock) {
      safeForceUnmap()
    }
  }

  /**
   * Do a basic sanity check on this index to detect obvious problems
   *
   * @throws CorruptIndexException if any problems are found
   */
  def sanityCheck(): Unit

  /**
   * Remove all the entries from the index.
   */
  protected def truncate(): Unit

  /**
   * Remove all entries from the index which have an offset greater than or equal to the given offset.
   * Truncating to an offset larger than the largest in the index has no effect.
   */
  def truncateTo(offset: Long): Unit

  /**
   * Remove all the entries from the index and resize the index to the max index size.
   */
  def reset(): Unit = {
    truncate()
    resize(maxIndexSize)
  }

  /**
   * Get offset relative to base offset of this index
   * @throws IndexOffsetOverflowException
   *
   * 计算相对位移
   */
  def relativeOffset(offset: Long): Int = {
    val relativeOffset = toRelative(offset)
    if (relativeOffset.isEmpty)
      throw new IndexOffsetOverflowException(s"Integer overflow for offset: $offset (${file.getAbsoluteFile})")
    relativeOffset.get
  }

  /**
   * Check if a particular offset is valid to be appended to this index.
   * @param offset The offset to check
   * @return true if this offset is valid to be appended to this index; false otherwise
   */
  def canAppendOffset(offset: Long): Boolean = {
    toRelative(offset).isDefined
  }

  protected def safeForceUnmap(): Unit = {
    try forceUnmap()
    catch {
      case t: Throwable => error(s"Error unmapping index $file", t)
    }
  }

  /**
   * Forcefully free the buffer's mmap.
   */
  protected[log] def forceUnmap() {
    try MappedByteBuffers.unmap(file.getAbsolutePath, mmap)
    finally mmap = null // Accessing unmapped mmap crashes JVM by SEGV so we null it out to be safe
  }

  /**
   * Execute the given function in a lock only if we are running on windows. We do this
   * because Windows won't let us resize a file while it is mmapped. As a result we have to force unmap it
   * and this requires synchronizing reads.
   */
  protected def maybeLock[T](lock: Lock)(fun: => T): T = {
    if (OperatingSystem.IS_WINDOWS)
      lock.lock()
    try fun
    finally {
      if (OperatingSystem.IS_WINDOWS)
        lock.unlock()
    }
  }

  /**
   * To parse an entry in the index.
   *
   * @param buffer the buffer of this memory mapped index.
   * @param n the slot
   * @return the index entry stored in the given slot.
   *
   * 返回索引中的相应条目
   * 参数 buffer: 索引文件mmap的buffer
   * 参数 n: 条目的下标
   * 返回 对应的日志条目
   */
  protected def parseEntry(buffer: ByteBuffer, n: Int): IndexEntry

  /**
   * Find the slot in which the largest entry less than or equal to the given target key or value is stored.
   * The comparison is made using the `IndexEntry.compareTo()` method.
   *
   * @param idx The index buffer
   * @param target The index key to look for
   * @return The slot found or -1 if the least entry in the index is larger than the target key or the index is empty
   *
   * 查找小于等于目标key(或value)的最大条目, 比较时使用IndexEntry.compareTo()方法
   * 参数 idx: 索引buffer
   * 参数 target: 查找的目标
   * 返回 相应的条目, 或者-1(如果索引中最小的条目都比目标大, 或索引为空)
   */
  protected def largestLowerBoundSlotFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchEntity): Int =
    indexSlotRangeFor(idx, target, searchEntity)._1

  /**
   * Find the smallest entry greater than or equal the target key or value. If none can be found, -1 is returned.
   * 查询大于等于目标key(或value)的最小索引槽, 如果没有则返回-1
   */
  protected def smallestUpperBoundSlotFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchEntity): Int =
    indexSlotRangeFor(idx, target, searchEntity)._2

  /**
   * Lookup lower and upper bounds for the given target.
   * 查找指定目标对应的索引区间(因为每个索引条目只对应一个点, 如果匹配则区间就为这个点, 否则为一个索引区间)
   */
  private def indexSlotRangeFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchEntity): (Int, Int) = {
    // check if the index is empty
    // 检查索引是否为空
    if(_entries == 0)
      return (-1, -1)

    // check if the target offset is smaller than the least offset
    // 检查范围
    if(compareIndexEntry(parseEntry(idx, 0), target, searchEntity) > 0)
      return (-1, 0)

    // binary search for the entry
    // 二分查找
    var lo = 0
    var hi = _entries - 1
    while(lo < hi) {
      val mid = ceil(hi/2.0 + lo/2.0).toInt
      //获取索引中对应的条目
      val found = parseEntry(idx, mid)
      //比较大小
      val compareResult = compareIndexEntry(found, target, searchEntity)
      //比较切分区间
      if(compareResult > 0)
        hi = mid - 1
      else if(compareResult < 0)
        lo = mid
      else
        return (mid, mid)
    }

    (lo, if (lo == _entries - 1) -1 else lo + 1)
  }

  /**
   * 比较索引条目与指定的key(或value)
   * @param indexEntry
   * @param target
   * @param searchEntity
   * @return
   */
  private def compareIndexEntry(indexEntry: IndexEntry, target: Long, searchEntity: IndexSearchEntity): Int = {
    searchEntity match {
      case IndexSearchType.KEY => indexEntry.indexKey.compareTo(target)
      case IndexSearchType.VALUE => indexEntry.indexValue.compareTo(target)
    }
  }

  /**
   * Round a number to the greatest exact multiple of the given factor less than the given number.
   * E.g. roundDownToExactMultiple(67, 8) == 64
   * 根据最小粒度factor计算小于等于number的最大值
   */
  private def roundDownToExactMultiple(number: Int, factor: Int) = factor * (number / factor)

  private def toRelative(offset: Long): Option[Int] = {
    val relativeOffset = offset - baseOffset
    if (relativeOffset < 0 || relativeOffset > Int.MaxValue)
      None
    else
      Some(relativeOffset.toInt)
  }

}

object IndexSearchType extends Enumeration {
  type IndexSearchEntity = Value
  val KEY, VALUE = Value
}

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


package kafka.server

import java.io.IOException
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap}

import kafka.utils.Logging

/*
 * LogDirFailureChannel allows an external thread to block waiting for new offline log dirs.
 *
 * There should be a single instance of LogDirFailureChannel accessible by any class that does disk-IO operation.
 * If IOException is encountered while accessing a log directory, the corresponding class can add the log directory name
 * to the LogDirFailureChannel using maybeAddOfflineLogDir(). Each log directory will be added only once. After a log
 * directory is added for the first time, a thread which is blocked waiting for new offline log directories
 * can take the name of the new offline log directory out of the LogDirFailureChannel and handle the log failure properly.
 * An offline log directory will stay offline until the broker is restarted.
 *
 * LogDirFailureChannel允许外部线程阻塞等待新的offline日志目录
 *
 * 应该存在一个LogDirFailureChannel实例, 该实例可以被任何做磁盘IO的类访问. 如果在访问目录过程中发生IO异常, 这些类可以通过
 * maybeAddOfflineLogDir()方法增加该目录名称到LogDirFailureChannel中. 每个日志目录只会被增加一次. 一个日志目录第一次增加到
 * LogDirFailureChannel时, 阻塞等待目录offline的线程可以通过LogDirFailureChannel获取新offline的目录名称, 然后处理日志失败.
 * 一个offline的日志目录会一直保持offline直到broker重启.
 */
class LogDirFailureChannel(logDirNum: Int) extends Logging {

  private val offlineLogDirs = new ConcurrentHashMap[String, String]
  private val offlineLogDirQueue = new ArrayBlockingQueue[String](logDirNum)

  /*
   * If the given logDir is not already offline, add it to the
   * set of offline log dirs and enqueue it to the logDirFailureEvent queue
   */
  def maybeAddOfflineLogDir(logDir: String, msg: => String, e: IOException): Unit = {
    error(msg, e)
    if (offlineLogDirs.putIfAbsent(logDir, logDir) == null) {
      offlineLogDirQueue.add(logDir)
    }
  }

  /*
   * Get the next offline log dir from logDirFailureEvent queue.
   * The method will wait if necessary until a new offline log directory becomes available
   */
  def takeNextOfflineLogDir(): String = offlineLogDirQueue.take()

}

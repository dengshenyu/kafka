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

/**
 * Configuration parameters for the log cleaner
 * 
 * @param numThreads The number of cleaner threads to run
 * @param dedupeBufferSize The total memory used for log deduplication
 * @param dedupeBufferLoadFactor The maximum percent full for the deduplication buffer
 * @param maxMessageSize The maximum size of a message that can appear in the log
 * @param maxIoBytesPerSecond The maximum read and write I/O that all cleaner threads are allowed to do
 * @param backOffMs The amount of time to wait before rechecking if no logs are eligible for cleaning
 * @param enableCleaner Allows completely disabling the log cleaner
 * @param hashAlgorithm The hash algorithm to use in key comparison.
 *
 * 日志cleaner的配置参数
 *
 * 参数 numThreads: cleaner线程数
 * 参数 dedupeBufferSize: 用于日志解耦合的总内存大小
 * 参数 dedupeBufferLoadFactor: 解耦合缓冲区的负载因子
 * 参数 maxMessageSize: 日志中一个消息的最大大小
 * 参数 maxIoBytesPerSecond: 所有cleaner线程的IO读写带宽
 * 参数 backOffMs: 没有日志需要清理时下一次检查前的等待时间
 * 参数 enableCleaner: 是否关闭cleaner
 * 参数 hashAlgorithm: 在key比较时使用的哈希算法
 */
case class CleanerConfig(numThreads: Int = 1,
                         dedupeBufferSize: Long = 4*1024*1024L,
                         dedupeBufferLoadFactor: Double = 0.9d,
                         ioBufferSize: Int = 1024*1024,
                         maxMessageSize: Int = 32*1024*1024,
                         maxIoBytesPerSecond: Double = Double.MaxValue,
                         backOffMs: Long = 15 * 1000,
                         enableCleaner: Boolean = true,
                         hashAlgorithm: String = "MD5") {
}

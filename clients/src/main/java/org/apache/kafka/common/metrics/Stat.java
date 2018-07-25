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
package org.apache.kafka.common.metrics;

/**
 * A Stat is a quantity such as average, max, etc that is computed off the stream of updates to a sensor
 */
/**
 * 一个Stat是一个可衡量的指标(例如平均值, 最大值等等), 这个指标根据流不断更新
 */
public interface Stat {

    /**
     * Record the given value
     * @param config The configuration to use for this metric
     * @param value The value to record
     * @param timeMs The POSIX time in milliseconds this value occurred
     */
    public void record(MetricConfig config, double value, long timeMs);

}

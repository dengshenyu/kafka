/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.security.auth

import kafka.common.KafkaException
import org.apache.kafka.common.resource.{PatternType, ResourcePattern}

object Resource {
  val Separator = ":"
  val ClusterResourceName = "kafka-cluster"
  val ClusterResource = Resource(Cluster, Resource.ClusterResourceName, PatternType.LITERAL)
  val ProducerIdResourceName = "producer-id"
  val WildCardResource = "*"

  def fromString(str: String): Resource = {
    ResourceType.values.find(resourceType => str.startsWith(resourceType.name + Separator)) match {
      case None => throw new KafkaException("Invalid resource string: '" + str + "'")
      case Some(resourceType) =>
        val remaining = str.substring(resourceType.name.length + 1)

        PatternType.values.find(patternType => remaining.startsWith(patternType.name + Separator)) match {
          case Some(patternType) =>
            val name = remaining.substring(patternType.name.length + 1)
            Resource(resourceType, name, patternType)

          case None =>
            Resource(resourceType, remaining, PatternType.LITERAL)
        }
    }
  }
}

/**
 *
 * @param resourceType non-null type of resource.
 * @param name non-null name of the resource, for topic this will be topic name , for group it will be group name. For cluster type
 *             it will be a constant string kafka-cluster.
 * @param patternType non-null resource pattern type: literal, prefixed, etc.
 * 参数 resourceType: 资源类型(不为null)
 * 参数 name: 资源名称(非null), 对于主题来说为主题名称, 对于分组来说为分组名称, 对于集群来说为一个固定常量kafka-cluster
 * 参数 patternType: 资源模式类型(非null): 字面量, 带前缀, 等等.
 */
case class Resource(resourceType: ResourceType, name: String, patternType: PatternType) {

  if (patternType == PatternType.MATCH || patternType == PatternType.ANY)
    throw new IllegalArgumentException("patternType must not be " + patternType)

  if (patternType == PatternType.UNKNOWN)
    throw new IllegalArgumentException("patternType must not be UNKNOWN")

  /**
    * Create an instance of this class with the provided parameters.
    * Resource pattern type would default to PatternType.LITERAL.
    *
    * @param resourceType non-null resource type
    * @param name         non-null resource name
    * @deprecated Since 2.0, use [[kafka.security.auth.Resource(ResourceType, String, PatternType)]]
    */
  @deprecated("Use Resource(ResourceType, String, PatternType", "Since 2.0")
  def this(resourceType: ResourceType, name: String) {
    this(resourceType, name, PatternType.LITERAL)
  }

  def toPattern: ResourcePattern = {
    new ResourcePattern(resourceType.toJava, name, patternType)
  }

  override def toString: String = {
    resourceType.name + Resource.Separator + patternType + Resource.Separator + name
  }
}


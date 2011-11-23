/*
 * Copyright 2011 Hui Wen Han, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package me.huiwen.prefz
package shards

import scala.collection.mutable
import com.twitter.gizzard.shards.RoutingNode
import com.twitter.util.Time
import com.twitter.util.TimeConversions._

class ReadWriteShardAdapter(shard: RoutingNode[Shard])
  extends Shard {

  

  def bulkUnsafeInsertPreferences(prefs: Seq[Preference]) = shard.writeOperation(_.bulkUnsafeInsertPreferences(prefs))

  def writeCopies(edges: Seq[Preference]) = shard.writeOperation(_.writeCopies(edges))

  def add(userId: Long, itemId: Long, source: String, action: String, updatedAt: Time, score: Double,
    status: Status, createType: CreateType) = shard.writeOperation(_.add(userId, itemId, source, action, updatedAt, score, status, createType))

  def update(userId: Long, itemId: Long, source: String, action: String, updatedAt: Time, score: Double,
    status: Status, createType: CreateType) = shard.writeOperation(_.update(userId, itemId, source, action, updatedAt, score, status, createType))

  def delete(userId: Long, itemId: Long, source: String, action: String) = shard.writeOperation(_.delete(userId, itemId, source, action))
}


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
package jobs

import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.shards.{ ShardId, RoutingNode }
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.Stats
import com.twitter.util.TimeConversions._
import conversions.Numeric._
import shards.{ Shard, ReadWriteShardAdapter }

object Copy {
  type CopyCursor = (Cursor, Cursor)

  val START = (Cursor.Start, Cursor.Start)
  val END = (Cursor.End, Cursor.End)
  val COUNT = 10000
}

class CopyFactory(nameServer: NameServer, scheduler: JobScheduler)
  extends CopyJobFactory[Shard] {
    

def apply(sourceShardId: ShardId, destinationShardId: ShardId) =
    new Copy(sourceShardId, destinationShardId, Copy.START, Copy.COUNT,
                     nameServer, scheduler)
 
}
class CopyParser(nameServer: NameServer, scheduler: JobScheduler)
  extends CopyJobParser[Shard] {
  def deserialize(attributes: Map[String, Any], sourceId: ShardId, destinationId: ShardId, count: Int) = {
    val cursor = (Cursor(attributes("cursor1").asInstanceOf[AnyVal].toLong),
      Cursor(attributes("cursor2").asInstanceOf[AnyVal].toLong))
    new Copy(sourceId, destinationId, cursor, count, nameServer, scheduler)
  }
}

class Copy(sourceShardId: ShardId, destinationShardId: ShardId, cursor: Copy.CopyCursor,
  count: Int, nameServer: NameServer, scheduler: JobScheduler)
  extends CopyJob[Shard](sourceShardId, destinationShardId, count, nameServer, scheduler) {

  def copyPage(source: RoutingNode[Shard], dest: RoutingNode[Shard], count: Int) = {
    val Seq(sourceShard, destinationShard) = Seq(source, dest) map { new ReadWriteShardAdapter(_) }

    val (items, newCursor) = sourceShard.selectAllPage(cursor, count)
    destinationShard.writeCopies(items)
    Stats.incr("edges-copy", items.size)
    if (newCursor == Copy.END) {
      None
    } else {
      Some(new Copy(sourceShardId, destinationShardId, newCursor, count, nameServer, scheduler))
    }
  }

  def serialize = Map("cursor1" -> cursor._1.position, "cursor2" -> cursor._2.position)
}

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

import com.twitter.gizzard.nameserver.MultiForwarder
import com.twitter.gizzard.shards.{RoutingNode, ShardException}
import me.huiwen.prefz.shards.{Shard, ReadWriteShardAdapter}


class ForwardingManager(val forwarder: MultiForwarder[Shard]) {
  @throws(classOf[ShardException])
  def find(userId: Long, graphId: Int): Shard = {
    new ReadWriteShardAdapter(findNode(userId, graphId))
  }

  @throws(classOf[ShardException])
  def findNode(userId: Long, graphId: Int)= {
    forwarder.find(graphId, userId)
  }


}

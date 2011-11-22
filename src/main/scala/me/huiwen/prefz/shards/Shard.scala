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
import com.twitter.gizzard.shards
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import com.twitter.gizzard.scheduler._
import me.huiwen.prefz.Preference
import me.huiwen.prefz.CreateType

trait Shard {

  @throws(classOf[shards.ShardException]) def get(userId: Long, itemId: Long): Option[Preference]

/*
  @throws(classOf[shards.ShardException]) def count(userId: Long, Status: Seq[Status]): Int

  @throws(classOf[shards.ShardException]) def selectAll(cursor: (Cursor, Cursor), count: Int): (Seq[Edge], (Cursor, Cursor))
  @throws(classOf[shards.ShardException]) def selectAllMetadata(cursor: Cursor, count: Int): (Seq[Metadata], Cursor)
  @throws(classOf[shards.ShardException]) def selectIncludingArchived(sourceId: Long, count: Int, cursor: Cursor): ResultWindow[Long]
  @throws(classOf[shards.ShardException]) def selectByDestinationId(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor): ResultWindow[Long]
  @throws(classOf[shards.ShardException]) def selectByPosition(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor): ResultWindow[Long]
  @throws(classOf[shards.ShardException]) def selectEdges(sourceId: Long, states: Seq[State], count: Int, cursor: Cursor): ResultWindow[Edge]
*/
  
  @throws(classOf[shards.ShardException]) def writeCopies(Preference: Seq[Preference])

  @throws(classOf[shards.ShardException]) def bulkUnsafeInsertPreferences(Preference: Seq[Preference])

  @throws(classOf[shards.ShardException]) def add(userId: Long, itemId: Long, source: String, action: String, updatedAt: Time, score: Double,
    status: Status, createType: CreateType)
  
  @throws(classOf[shards.ShardException]) def add(pref:Preference)
  
  @throws(classOf[shards.ShardException]) def delete(userId: Long, itemId: Long, source: String, action: String)
  
  @throws(classOf[shards.ShardException]) def delete(pref:Preference)
  
  @throws(classOf[shards.ShardException]) def update(userId: Long, itemId: Long, source: String, action: String, updatedAt: Time, score: Double,
    status: Status, createType: CreateType)

  @throws(classOf[shards.ShardException]) def update(pref:Preference)

}




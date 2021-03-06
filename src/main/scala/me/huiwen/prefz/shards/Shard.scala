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

  @throws(classOf[shards.ShardException]) def writeCopies(Preference: Seq[Preference])

  @throws(classOf[shards.ShardException]) def bulkUnsafeInsertPreferences(Preference: Seq[Preference])

  @throws(classOf[shards.ShardException]) def add(userId: Long, itemId: Long, source: String, action: String, score: Double,updatedAt: Time, 
    status: Status, createType: CreateType)

  @throws(classOf[shards.ShardException]) def addPreference(pref: Preference)

  @throws(classOf[shards.ShardException]) def delete(userId: Long, itemId: Long, source: String, action: String)

  @throws(classOf[shards.ShardException]) def deletePreference(pref: Preference)

  @throws(classOf[shards.ShardException]) def selectByUserItemSourceAndAction(
    userId: Long, itemId: Long, source: String, action: String):Option[Preference]
  
  @throws(classOf[shards.ShardException]) def selectByUserSourceAndAction(userId: Long, source: String, action: String): Seq[Preference]
  @throws(classOf[shards.ShardException]) def selectPageByUserSourceAndAction(userId: Long, source: String, action: String, cursor: Cursor, count: Int): (Seq[Preference], Cursor)
  
  @throws(classOf[shards.ShardException]) def selectBySourcAndAction(source: String, action: String): Seq[Preference]
  @throws(classOf[shards.ShardException]) def selectPageBySourcAndAction(source: String, action: String, cursor: (Cursor, Cursor), count: Int):(Seq[Preference],(Cursor,Cursor))
  

  @throws(classOf[shards.ShardException]) def selectUserIdsBySource(source: String)
  
  @throws(classOf[shards.ShardException]) def selectByUser(userId: Long):Seq[Preference]
   
  @throws(classOf[shards.ShardException]) def selectPageByUser(userId: Long, cursor: Cursor, count: Int):ResultWindow[Preference]
  @throws(classOf[shards.ShardException]) def selectAll():Seq[Preference]
  @throws(classOf[shards.ShardException]) def selectAllPage(cursor: (Cursor, Cursor), count: Int):(Seq[Preference], (Cursor,Cursor))

  @throws(classOf[shards.ShardException]) def update(userId: Long, itemId: Long, source: String, action: String,  score: Double,updatedAt: Time,
    status: Status, createType: CreateType)

  @throws(classOf[shards.ShardException]) def updatePreference(pref: Preference)

}




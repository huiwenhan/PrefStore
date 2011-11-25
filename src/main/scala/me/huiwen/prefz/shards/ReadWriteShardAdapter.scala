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

  

  def bulkUnsafeInsertPreferences(prefs: Seq[Preference]) 
  	= shard.write.all(_.bulkUnsafeInsertPreferences(prefs))

  def writeCopies(edges: Seq[Preference]) 
  	= shard.write.all(_.writeCopies(edges))

  def add(userId: Long, itemId: Long, source: String, action: String, score: Double,createdTime: Time, status: Status, createType: CreateType) 
  	= shard.write.all(_.add(userId,itemId,source,action,score,createdTime,status,createType))


  def addPreference(pref: Preference) 
  	= shard.write.all(_.addPreference(pref))

  def delete(userId: Long, itemId: Long, source: String, action: String)
  	=shard.write.all(_.delete(userId,itemId,source,action))

  def deletePreference(pref: Preference)
  	=shard.write.all(_.deletePreference(pref))
  	
  def update(userId: Long, itemId: Long, source: String, action: String,  score: Double,createdTime: Time,status: Status, createType: CreateType)
  	= shard.write.all(_.update(userId,itemId,source,action,score,createdTime,status,createType))
  def updatePreference(pref: Preference)
  = shard.write.all(_.updatePreference(pref))
  
  def selectByUserItemSourceAndAction(userId: Long, itemId: Long, source: String, action: String) 
  	=shard.read.any(_.selectByUserItemSourceAndAction(userId,itemId,source,action))
  
  def selectByUserSourceAndAction(userId: Long, source: String, action: String)
  	=shard.read.any(_.selectByUserSourceAndAction(userId,source,action))
  	
  def selectPageByUserSourceAndAction(userId: Long, source: String, action: String, cursor: Cursor, count: Int): (Seq[Preference], Cursor)
  	=shard.read.any(_.selectPageByUserSourceAndAction(userId,source,action,cursor,count))
  	
  def selectBySourcAndAction(source: String, action: String)
  	=shard.read.any(_.selectBySourcAndAction(source,action))
  	
  def selectBySourcAndAction(source: String, action: String, cursor: (Cursor, Cursor), count: Int)
   =shard.read.any(_.selectBySourcAndAction(source,action,cursor,count))

 def selectUserIdsBySource(source: String)
  =shard.read.any(_.selectUserIdsBySource(source))

def selectAll(cursor: (Cursor, Cursor), count: Int):(Seq[Preference], (Cursor,Cursor))
 =shard.read.any(_.selectAll(cursor,count))
  
def selectByUser(userId: Long):ResultWindow[Preference]
   =shard.read.any(_.selectByUser(userId))

}


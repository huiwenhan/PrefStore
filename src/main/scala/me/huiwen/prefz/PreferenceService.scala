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

import com.twitter.gizzard.Stats
import com.twitter.gizzard.util.Future
import com.twitter.gizzard.nameserver.{NameServer, NonExistentShard, InvalidShard}
import com.twitter.gizzard.scheduler.{CopyJobFactory, JsonJob, PrioritizingJobScheduler}
import com.twitter.gizzard.shards.{ShardBlackHoleException, ShardDatabaseTimeoutException,
  ShardOfflineException, ShardTimeoutException}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.logging.Logger
import thrift.PrefException
import com.twitter.util.Time

class PreferenceService(
  forwardingManager: ForwardingManager,
  schedule: PrioritizingJobScheduler,
  future: Future,
  aggregateJobsPageSize: Int) {

  private val log = Logger.get(getClass.getName)
  private val exceptionLog = Logger.get("exception")

  def shutdown() {
    schedule.shutdown()
    future.shutdown()
  }

    def create(graphId:Int,userId: Long, itemId: Long, source: String, action: String,  score: Double,createdTime: Int,
    status: Status, createType: CreateType)
    {
      rethrowExceptionsAsThrift
      {
        val shard = forwardingManager.find(userId, graphId)
        shard.add(userId,itemId,source,action,score,Time(createdTime),status,createType);
      }
    }

    def create(graphId:Int,pref: Preference)
    {
       rethrowExceptionsAsThrift
      {
        val shard = forwardingManager.find(pref.userId, graphId)
        shard.add(pref);
      }
    }


    def delete(graphId:Int,userId: Long, itemId: Long, source: String, action: String)
    {
       rethrowExceptionsAsThrift
      {
        val shard = forwardingManager.find(userId, graphId)
        shard.delete(userId,itemId,source,action)
      }
    }

    def delete(graphId:Int,pref: Preference)
    {
       rethrowExceptionsAsThrift
       {
        val shard = forwardingManager.find(pref.userId, graphId)
        shard.delete(pref);
      }
    }
    
   def update(graphId:Int,userId: Long, itemId: Long, source: String, action: String, score: Double,updatedAt: Time, 
    status: Status, createType: CreateType)
   {
      rethrowExceptionsAsThrift
      {
        val shard = forwardingManager.find(userId, graphId)
        shard.add(userId,itemId,source,action,score,updatedAt,status,createType);
      }
      
   }

  def update(graphId:Int,pref: Preference)
  {
           rethrowExceptionsAsThrift
       {
        val shard = forwardingManager.find(pref.userId, graphId)
        shard.update(pref);
      }
  }

  def selectByUserItemSourceAndAction(graphId:Int,userId: Long, itemId: Long, source: String, action: String):Option[Preference] = 
  {
     rethrowExceptionsAsThrift {
        val shard = forwardingManager.find(userId, graphId)
        shard.selectByUserItemSourceAndAction(userId,itemId,source,action)     
    }   
  }
  def selectByUserSourceAndAction(userId: Long, source: String, action: String)
  {
    
  }
  def selectPageByUserSourceAndAction(graphId:Int,userId: Long, source: String, action: String, cursor: Cursor, count: Int)
  {
     rethrowExceptionsAsThrift {
        val shard = forwardingManager.find(userId, graphId)
        shard.selectPageByUserSourceAndAction(userId,source,action,cursor,count)     
    }   
  }
  
   def selectBySourcAndAction(source: String, action: String)
   {
     
     
   }
  def selectBySourcAndAction(graphId:Int,source: String, action: String, cursor: (Cursor, Cursor), count: Int)
  {
       rethrowExceptionsAsThrift {
        val shard = forwardingManager.find(0, graphId)
        shard.selectBySourcAndAction(source,action,cursor,count)     
    }  
    
  }
  

 def selectUserIdsBySource(source: String)
 {
  
 }
  


  private def countAndRethrow(e: Throwable) = {
    Stats.incr("exceptions-" + e.getClass.getName.split("\\.").last)
    throw(new PrefException(e.getMessage))
  }

  private def rethrowExceptionsAsThrift[A](block: => A): A = {
    try {
      block
    } catch {
      case e: NonExistentShard =>
        log.error(e, "NonexistentShard: %s", e)
        throw(new PrefException(e.getMessage))
      case e: InvalidShard =>
        log.error(e, "NonexistentShard: %s", e)
        throw(new PrefException(e.getMessage))
      case e: PrefException =>
        Stats.incr(e.getClass.getName)
        throw(e)
      case e: ShardTimeoutException =>
        countAndRethrow(e)
      case e: ShardDatabaseTimeoutException =>
        countAndRethrow(e)
      case e: ShardOfflineException =>
        countAndRethrow(e)
      case e: Throwable =>
        Stats.incr("exceptions-unknown")
        exceptionLog.error(e, "Unhandled error in PreferenceService", e)
        log.error("Unhandled error in PreferenceService: " + e.toString)
        throw(new PrefException(e.toString))
    }
  }
}

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
        shard.add(userId,itemId,source,action,score,Time.fromSeconds(createdTime),status,createType);
      }
    }

    def createPreference(graphId:Int,pref: Preference)
    {
       rethrowExceptionsAsThrift
      {
        val shard = forwardingManager.find(pref.userId, graphId)
        shard.addPreference(pref);
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

    def deletePreference(graphId:Int,pref: Preference)
    {
       rethrowExceptionsAsThrift
       {
        val shard = forwardingManager.find(pref.userId, graphId)
        shard.deletePreference(pref);
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

  def updatePreference(graphId:Int,pref: Preference)
  {
           rethrowExceptionsAsThrift
       {
        val shard = forwardingManager.find(pref.userId, graphId)
        shard.updatePreference(pref);
      }
  }

  def selectByUserItemSourceAndAction(graphId:Int,userId: Long, itemId: Long, source: String, action: String):Preference = 
  {
     rethrowExceptionsAsThrift {
        val shard = forwardingManager.find(userId, graphId)
        shard.selectByUserItemSourceAndAction(userId,itemId,source,action)
        .getOrElse {
        throw new PrefException("Record not found: (%d, %d)".format(userId,itemId))
      }
    }   
  }
  def selectByUserSourceAndAction(graphId:Int,userId: Long, source: String, action: String):Seq[Preference]=
  {
    throw(new PrefException(""))
  }
  def selectPageByUserSourceAndAction(graphId:Int,userId: Long, source: String, action: String, cursor: Cursor, count: Int)
  	:(Seq[Preference],Cursor)=
  {
     rethrowExceptionsAsThrift {
        val shard = forwardingManager.find(userId, graphId)
        shard.selectPageByUserSourceAndAction(userId,source,action,cursor,count)     
    }   
  }
  
   def selectBySourcAndAction(graphId:Int,source: String, action: String): Seq[Preference]=
   {
     rethrowExceptionsAsThrift {
        val shard = forwardingManager.find(0, graphId)
        shard.selectBySourcAndAction(source,action)     
    }  
   }
  def selectPageBySourcAndAction(graphId:Int,source: String, action: String, cursor: (Cursor, Cursor), count: Int)
  {
       rethrowExceptionsAsThrift {
        val shard = forwardingManager.find(0, graphId)
        shard.selectPageBySourcAndAction(source,action,cursor,count)     
    }  
    
  }
  

 def selectUserIdsBySource(source: String): List[Preference]=
 {
  throw(new PrefException(""))
 }
  

  def selectByUser(graphId:Int,userId: Long):Seq[Preference]=
  {
   
    rethrowExceptionsAsThrift {
        val shard = forwardingManager.find(0, graphId)
        shard.selectByUser(userId)     
    }  
  }
   
  def selectPageByUser(graphId:Int,userId: Long, cursor: Cursor, count: Int):ResultWindow[Preference]=
  {
    rethrowExceptionsAsThrift {
        val shard = forwardingManager.find(0, graphId)
        shard.selectPageByUser(userId,cursor,count)     
    }  
  }
  def selectAll(graphId:Int):Seq[Preference]=
  {
    rethrowExceptionsAsThrift {
        val shard = forwardingManager.find(0, graphId)
        shard.selectAll()     
    }  
  }
 def selectAllPage(graphId:Int,cursor: (Cursor, Cursor), count: Int):(Seq[Preference],(Cursor,Cursor))={
   rethrowExceptionsAsThrift {
        val shard = forwardingManager.find(0, graphId)
        shard.selectAllPage(cursor,count)     
    }  
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

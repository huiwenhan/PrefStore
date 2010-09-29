package com.ibm.webahead.steer.gizzard.preference

import com.twitter.gizzard.shards
import com.twitter.gizzard.shards.ShardId
import com.twitter.gizzard.shards.ReadWriteShard
import com.twitter.xrayspecs.Time
import com.twitter.results.{Cursor, ResultWindow}


class ReadWriteShardAdapter(shard: ReadWriteShard[Shard])
  extends shards.ReadWriteShardAdapter(shard) with Shard {

  def create(userId:Long,
		  itemId:Long,score:Double,
		  source:String,action:String,
		  create: Int, 
		  createType: Int) = 
		 	  shard.writeOperation(_.create( userId,
		 	 		 		  itemId,score,
		 	 		 		  source,action,
		 	 		 		  create,createType))
  def destroy(pref: Preference)              = shard.writeOperation(_.destroy(pref))
  def write(prefs: Seq[Preference])                    = shard.writeOperation(_.write(prefs))

  def read(id: Long)                           = shard.readOperation(_.read(id))
  def selectAll(cursor: Cursor, count: Int)    = shard.readOperation(_.selectAll(cursor, count))
}

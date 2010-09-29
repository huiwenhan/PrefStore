package com.ibm.webahead.steer.gizzard.preference.jobs

import com.twitter.gizzard
import com.twitter.results.{Cursor, ResultWindow}
import com.twitter.gizzard.shards.ShardId
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.JobScheduler
import com.twitter.xrayspecs.TimeConversions._


object CopyFactory extends com.twitter.gizzard.jobs.CopyFactory[Shard] {
  val COUNT = 500

  def apply(sourceShardId: ShardId, destinationShardId: ShardId) = new Copy(sourceShardId, destinationShardId, Cursor.Start, COUNT)
}

object CopyParser extends com.twitter.gizzard.jobs.CopyParser[Shard] {
  def apply(attributes: Map[String, Any]) = {
    val casted = attributes.asInstanceOf[Map[String, AnyVal]]
    new Copy(
      casted("source_shard_id").asInstanceOf[ShardId],
      casted("destination_shard_id").asInstanceOf[ShardId],
      casted("cursor").asInstanceOf[Cursor],
      casted("count").toInt)
  }
}

class Copy(sourceShardId: ShardId, destinationShardId: ShardId, cursor: Cursor, count: Int) 
	extends com.twitter.gizzard.jobs.Copy[preference.Shard](sourceShardId, destinationShardId, count) {
  def serialize = Map("cursor" -> cursor)

  def copyPage(sourceShard: preference.Shard, 
		  destinationShard: preference.Shard, count: Int) = {
    val (items, nextCursor) = sourceShard.selectAll(cursor, count)
    destinationShard.write(items)
    nextCursor.map(new Copy(sourceShardId, destinationShardId, _, count))
  }
}

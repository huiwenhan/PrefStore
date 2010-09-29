package com.ibm.webahead.steer.gizzard.preference.integration

import com.twitter.gizzard.thrift.ShardManagerService
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.Logger
import com.twitter.gizzard._
import com.twitter.gizzard.shards._
import com.twitter.gizzard.nameserver.Forwarding
import com.twitter.gizzard.thrift.conversions.ShardInfo._



object ShardManagerSpec extends ConfiguredSpecification {
  "ShardManager" should {
    import Database._
    Time.freeze()
    val state = Preferencez(config, w3c, databaseFactory)
    val prefzService = state.prefzService
    val shardManagerService = new ShardManagerService(state.nameServer, state.copyFactory, state.prioritizingScheduler(2))
    val shardIdA= new shards.ShardId("localhost","shard_a")
    val shardIdB= new shards.ShardId("localhost","shard_b")
    val replicatingShardId= new shards.ShardId("localhost","replicating")
    val shardInfoA = new ShardInfo(shardIdA,"com.ibm.webahead.steer.gizzard.preference.SqlShard", "", "",shards.Busy.Normal )
    val shardInfoB = new ShardInfo(shardIdB,"com.ibm.webahead.steer.gizzard.preference.SqlShard", "", "",shards.Busy.Normal)
    val replicatingShardInfo = new ShardInfo(replicatingShardId,
    		"com.twitter.gizzard.shards.ReplicatingShard", "", "",shards.Busy.Normal)
    val queryEvaluator = queryEvaluatorFactory("localhost", null, config("prefz.db.username"), config("prefz.db.password"))

    doBefore {
      queryEvaluator.execute("DROP DATABASE IF EXISTS " + config("prefz.nameserver.name"))
      queryEvaluator.execute("CREATE DATABASE " + config("prefz.nameserver.name"))
      state.nameServer.rebuildSchema()
      queryEvaluator.execute("DROP DATABASE IF EXISTS " + config("prefz.db.name"))

      state.nameServer.createShard(shardInfoA)
      state.nameServer.createShard(shardInfoB)
      state.nameServer.createShard(replicatingShardInfo)

      val weight = 1
      state.nameServer.addLink(replicatingShardId, shardIdA, weight)
      state.nameServer.addLink(replicatingShardId, shardIdB, weight)
      state.nameServer.setForwarding(new Forwarding(0, Math.MIN_LONG, replicatingShardId))
      state.start()
    }

    "replication" in {
      prefzService.create(1,1,1,"TAP","computed", Time.now.inSeconds, 1)
      prefzService.read(1) must eventually(not(throwA[Exception]))

      val thriftReplicatingShardId =thrift.conversions.ShardId.
      	shardIdToRichShardId(replicatingShardId).toThrift
      val thriftShardIdA =thrift.conversions.ShardId.
      	shardIdToRichShardId(shardIdA).toThrift
      	
      val thriftShardIdB =thrift.conversions.ShardId.
      	shardIdToRichShardId(shardIdB).toThrift
      shardManagerService.replace_forwarding(thriftReplicatingShardId, thriftShardIdA)
      shardManagerService.reload_forwardings()
      prefzService.read(1) must eventually(not(throwA[Exception]))

      shardManagerService.replace_forwarding(thriftReplicatingShardId, thriftShardIdB)
      shardManagerService.reload_forwardings()
      prefzService.read(1) must eventually(not(throwA[Exception]))
    }

    "shard migration" in {
      val id = prefzService.create(1,1,1,"TAP","computed", Time.now.inSeconds, 1)
      prefzService.read(1) must eventually(not(throwA[Exception]))

      val sourceShardInfo = shardInfoA
      val destinationShardInfo = new ShardInfo("com.ibm.webahead.steer.gizzard.preference.SqlShard", "shard_c" + 0, "localhost")
      //val migration = shardManagerService.setup_migration(sourceShardInfo.toThrift, destinationShardInfo.toThrift)
      //shardManagerService.reload_forwardings()
      //shardManagerService.migrate_shard(migration)
      //shardManagerService.replace_forwarding(replicatingShardId, migration.destination_shard_id)
      shardManagerService.reload_forwardings()
      prefzService.read(1) must eventually(not(throwA[Exception]))
    }
  }
}

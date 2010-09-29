package com.ibm.webahead.steer.gizzard.preference.integration

import com.twitter.gizzard.thrift.ShardManagerService
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.Logger
import com.twitter.gizzard.shards.ShardInfo
import com.twitter.gizzard.shards.ShardId
import com.twitter.gizzard.nameserver.Forwarding


object PrefzSpec extends ConfiguredSpecification {
  "Prefz" should {
    import Database._
    Time.freeze()
    val state = Preferencez(config, w3c, databaseFactory)
    val prefzService = state.prefzService
    val shardId = new  ShardId("localhost","shard_a")
    val shardInfo = new ShardInfo("com.ibm.webahead.steer.gizzard.preference.SqlShard", "shard_a", "localhost")
    val queryEvaluator = queryEvaluatorFactory("localhost", null, config("prefz.db.username"), config("prefz.db.password"))

    doBefore {
      queryEvaluator.execute("DROP DATABASE IF EXISTS " + config("prefz.nameserver.name"))
      queryEvaluator.execute("CREATE DATABASE " + config("prefz.nameserver.name"))
      state.nameServer.rebuildSchema()
      queryEvaluator.execute("DROP DATABASE IF EXISTS " + config("prefz.db.name"))

      state.nameServer.createShard(shardInfo)
      state.nameServer.setForwarding(new Forwarding(0, Math.MIN_LONG, shardId))
      state.start()
    }

    "row create & read" in {
      prefzService.create(1,1,9999,"TAP","computed", 2, 1)
      prefzService.read(1) must eventually(not(throwA[Exception]))
      val prefs = prefzService.read(1)
      prefs.size mustEqual 1
      prefzService.destroy(prefs.get(0))
      prefzService.read(1) must eventually(throwA[Exception])
    }
  }
}

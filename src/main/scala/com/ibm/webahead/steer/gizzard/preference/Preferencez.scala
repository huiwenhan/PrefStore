package com.ibm.webahead.steer.gizzard.preference

import net.lag.configgy.ConfigMap
import com.twitter.querulous.database.{ApachePoolingDatabaseFactory, MemoizingDatabaseFactory, DatabaseFactory}
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.evaluator.StandardQueryEvaluatorFactory
import com.twitter.xrayspecs.TimeConversions._

import net.lag.logging.{Logger}
//import net.lag.logging.{Logger, ThrottledLogger}
import com.twitter.gizzard.Future
import com.twitter.gizzard.scheduler.{JobScheduler, PrioritizingJobScheduler}
import com.twitter.gizzard.shards._
import com.twitter.gizzard.nameserver
import com.twitter.gizzard.jobs.{PolymorphicJobParser, BoundJobParser}
import scala.collection.mutable
import com.twitter.ostrich.W3CStats



object Preferencez {
  case class State(
    prefzService: PrefzService,
    prioritizingScheduler: PrioritizingJobScheduler,
    nameServer: nameserver.NameServer[Shard],
    copyFactory: com.twitter.gizzard.jobs.CopyFactory[Shard]) {
      def start() = {
        nameServer.reload()
        prioritizingScheduler.start()
      }

      def shutdown() = prioritizingScheduler.shutdown()
    }

  def apply(config: ConfigMap, w3c: W3CStats): State = apply(
    config, w3c,
    new MemoizingDatabaseFactory(new ApachePoolingDatabaseFactory(
      config("prefz.db.connection_pool.size_min").toInt,
      config("prefz.db.connection_pool.size_max").toInt,
      config("prefz.db.connection_pool.test_idle_msec").toLong.millis,
      config("prefz.db.connection_pool.max_wait").toLong.millis,
      config("prefz.db.connection_pool.test_on_borrow").toBoolean,
      config("prefz.db.connection_pool.min_evictable_idle_msec").toLong.millis))
  )

  def apply(config: ConfigMap, w3c: W3CStats, databaseFactory: DatabaseFactory): State = {
    val queryEvaluatorFactory    = new StandardQueryEvaluatorFactory(databaseFactory, new SqlQueryFactory)

    //val throttledLogger         = new ThrottledLogger[String](Logger(), config("throttled_log.period_msec").toInt, config("throttled_log.rate").toInt)
    val future                  = new Future("ReplicatingFuture", config.configMap("prefz.replication.future"))

    val shardRepository         = new nameserver.BasicShardRepository[Shard](new ReadWriteShardAdapter(_), future)
    shardRepository             += ("com.ibm.webahead.steer.gizzard.preference.SqlShard" -> new SqlShardFactory(queryEvaluatorFactory, config))

    val nameServerShards = config.getList("prefz.nameserver.hostnames").map { hostname =>
      new nameserver.SqlShard(
        queryEvaluatorFactory(
          hostname,
          config("prefz.nameserver.name"),
          config("prefz.nameserver.username"),
          config("prefz.nameserver.password")))
    }

    val replicatingNameServerShard = new nameserver.ReadWriteShardAdapter(new ReplicatingShard(
      new ShardInfo("com.twitter.gizzard.shards.ReplicatingShard", "", ""),
      1, nameServerShards, new nameserver.LoadBalancer(nameServerShards), future))
    val nameServer                 = new nameserver.NameServer(replicatingNameServerShard, shardRepository, Hash)
    val forwardingManager          = new ForwardingManager(nameServer)

    val polymorphicJobParser    = new PolymorphicJobParser
    val schedulerMap = new mutable.HashMap[Int, JobScheduler]
    List((3, "high"), (2, "medium"), (1, "low")).foreach { case (priority, configName) =>
      val queueConfig = config.configMap("prefz.queue")
      //val scheduler = JobScheduler(configName, queueConfig, polymorphicJobParser, w3c)
      val scheduler = JobScheduler(configName,  queueConfig,polymorphicJobParser)
      schedulerMap(priority) = scheduler
    }
    val prioritizingScheduler = new PrioritizingJobScheduler(schedulerMap)

    val copyJobParser           = new BoundJobParser(jobs.CopyParser, (nameServer, prioritizingScheduler(2)))
    //val migrateJobParser        = new BoundJobParser(new com.twitter.gizzard.jobs.MigrateParser(jobs.CopyParser), (nameServer, prioritizingScheduler(2)))
    val createJobParser         = new BoundJobParser(jobs.CreateParser, forwardingManager)
    val destroyJobParser        = new BoundJobParser(jobs.DestroyParser, forwardingManager)
    polymorphicJobParser        += ("Copy".r, copyJobParser)
    //polymorphicJobParser        += ("Migrate".r, migrateJobParser)
    polymorphicJobParser        += ("Create".r, createJobParser)
    polymorphicJobParser        += ("Destroy".r, destroyJobParser)

    val prefService             = new PrefzService(config,queryEvaluatorFactory,nameServer,forwardingManager, prioritizingScheduler, new IdGenerator(config("host.id").toInt))

    State(prefService, prioritizingScheduler, nameServer, com.ibm.webahead.steer.gizzard.preference.jobs.CopyFactory)
  }
}
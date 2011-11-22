/*
 * Copyright 2010 Twitter, Inc.
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

import com.twitter.util.Duration
import com.twitter.ostrich.admin.Service
import com.twitter.querulous.StatsCollector
import com.twitter.gizzard.GizzardServer
import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.proxy.ExceptionHandlingProxyFactory
import com.twitter.gizzard.Stats
import me.huiwen.prefz.shards.{Shard, SqlShardFactory}
import me.huiwen.prefz.config.{PrefStore => PrefStoreConfig}


class PrefStore(config: PrefStoreConfig) extends GizzardServer(config) with Service {
  object PrefzExceptionWrappingProxyFactory extends ExceptionHandlingProxyFactory[thrift.PreferenceService.Iface]({ (flock, e) =>
    e match {
      case _: thrift.PrefzException =>
        throw e
      case _ =>
        exceptionLog.error(e, "Error in PrefStore.")
        throw new thrift.PrefzException(e.toString)
    }
  })

  val stats = new StatsCollector {
    def incr(name: String, count: Int) = Stats.incr(name, count)
    def time[A](name: String)(f: => A): A = {
      val (rv, duration) = Duration.inMilliseconds(f)
      Stats.addMetric(name, duration.inMillis.toInt)
      rv
    }
    override def addGauge(name: String)(gauge: => Double) { Stats.addGauge(name)(gauge) }
  }

  val jobPriorities = List(Priority.Low, Priority.Medium, Priority.High).map(_.id)
  val copyPriority = Priority.Medium.id

  val shardFactory = new SqlShardFactory(
    config.edgesQueryEvaluator(
      stats,
      new TransactionStatsCollectingDatabaseFactory(_),
      new TransactionStatsCollectingQueryFactory(_)
    ),
    config.lowLatencyQueryEvaluator(
      stats,
      new TransactionStatsCollectingDatabaseFactory(_),
      new TransactionStatsCollectingQueryFactory(_)
    ),
    config.materializingQueryEvaluator(stats),
    config.databaseConnection
  )

  nameServer.configureMultiForwarder[Shard] {
    _.shardFactories(
      "com.twitter.flockdb.SqlShard" -> shardFactory,
      "com.twitter.service.flock.edges.SqlShard" -> shardFactory
    )
    .copyFactory(new jobs.CopyFactory(nameServer, jobScheduler(Priority.Medium.id)))
  }

  val forwardingManager = new ForwardingManager(nameServer.multiTableForwarder[Shard])

  jobCodec += ("single.Single".r, new jobs.single.SingleJobParser(forwardingManager, OrderedUuidGenerator))
  jobCodec += ("multi.Multi".r,   new jobs.multi.MultiJobParser(forwardingManager, jobScheduler, config.aggregateJobsPageSize))

  jobCodec += ("jobs\\.(Copy|Migrate)".r,                 new jobs.CopyParser(nameServer, jobScheduler(Priority.Medium.id)))
  jobCodec += ("jobs\\.(MetadataCopy|MetadataMigrate)".r, new jobs.MetadataCopyParser(nameServer, jobScheduler(Priority.Medium.id)))

 
  val flockService = {
    val edges = new PreferenceService(
      forwardingManager,
      jobScheduler,
      config.readFuture("readFuture"),
      config.aggregateJobsPageSize
    )

    new PrefDBThriftAdapter(edges, jobScheduler)
  }

  private val loggingProxy = makeLoggingProxy[thrift.PreferenceService.Iface]()
  lazy val loggingFlockService = loggingProxy(flockService)

  lazy val flockThriftServer = {
    val processor = new thrift.PreferenceService.Processor(
      PrefzExceptionWrappingProxyFactory(
        loggingFlockService))

    config.server(processor)
  }

  // satisfy service

  def start() {
    startGizzard()
    val runnable = new Runnable { def run() { flockThriftServer.serve() } }
    new Thread(runnable, "FlockDBServerThread").start()
  }

  def shutdown() {
    flockThriftServer.stop()
    shutdownGizzard(false)
  }

  override def quiesce() {
    flockThriftServer.stop()
    shutdownGizzard(true)
  }
}

class PrefStoreThriftAdapter(val prefs: PreferenceService, val scheduler: PrioritizingJobScheduler) extends thrift.PreferenceService.Iface {
  import java.util.{List => JList}
  import scala.collection.JavaConversions._
  import com.twitter.gizzard.thrift.conversions.Sequences._
  import me.huiwen.prefz.conversions.Preference._
  import me.huiwen.prefz.conversions.Page._
  import me.huiwen.prefz.conversions.Results._
  import com.twitter.gizzard.shards.ShardException
  import thrift.PrefzException

  def contains(source_id: Long, graph_id: Int, destination_id: Long) = {
    prefs.contains(source_id, graph_id, destination_id)
  }

  def get(source_id: Long, graph_id: Int, destination_id: Long) = {
    prefs.get(source_id, graph_id, destination_id).toThrift
  }

  @deprecated("Use `select2` instead")
  def select(operations: JList[thrift.SelectOperation], page: thrift.Page): thrift.Results = {
    prefs.select(new SelectQuery(operations.toSeq.map { _.fromThrift }, page.fromThrift)).toThrift
  }

  def select2(queries: JList[thrift.SelectQuery]): JList[thrift.Results] = {
    prefs.select(queries.toSeq.map { _.fromThrift }).map { _.toThrift }
  }

  def select_edges(queries: JList[thrift.EdgeQuery]) = {
    prefs.selectEdges(queries.toSeq.map { _.fromThrift }).map { _.toEdgeResults }
  }

  def execute(operations: thrift.ExecuteOperations) = {
    try {
      prefs.execute(operations.fromThrift)
    } catch {
      case e: ShardException =>
        throw new PrefzException(e.toString)
    }
  }

  @deprecated("Use `count2` instead")
  def count(query: JList[thrift.SelectOperation]) = {
    edges.count(List(query.toSeq.map { _.fromThrift })).first
  }

  def count2(queries: JList[JList[thrift.SelectOperation]]) = {
    edges.count(queries.toSeq.map { _.toSeq.map { _.fromThrift }}).pack
  }
}

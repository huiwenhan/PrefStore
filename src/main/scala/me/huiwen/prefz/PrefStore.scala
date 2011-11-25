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

import com.twitter.util.Duration
import com.twitter.util.Time
import com.twitter.ostrich.admin.Service
import com.twitter.querulous.StatsCollector
import com.twitter.gizzard.GizzardServer
import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.proxy.ExceptionHandlingProxyFactory
import com.twitter.gizzard.Stats
import me.huiwen.prefz.shards.{ Shard, SqlShardFactory }
import me.huiwen.prefz.config.{ PrefStore => PrefStoreConfig }

class PrefStore(config: PrefStoreConfig) extends GizzardServer(config) with Service {
  object PrefzExceptionWrappingProxyFactory extends ExceptionHandlingProxyFactory[thrift.PreferenceService.Iface]({ (flock, e) =>
    e match {
      case _: thrift.PrefException =>
        throw e
      case _ =>
        exceptionLog.error(e, "Error in PrefStore.")
        throw new thrift.PrefException(e.toString)
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
      new TransactionStatsCollectingQueryFactory(_)),
    config.lowLatencyQueryEvaluator(
      stats,
      new TransactionStatsCollectingDatabaseFactory(_),
      new TransactionStatsCollectingQueryFactory(_)),
    config.materializingQueryEvaluator(stats),
    config.databaseConnection)

  nameServer.configureMultiForwarder[Shard] {
    _.shardFactories(
      "com.twitter.flockdb.SqlShard" -> shardFactory,
      "com.twitter.service.flock.edges.SqlShard" -> shardFactory)
      .copyFactory(new jobs.CopyFactory(nameServer, jobScheduler(Priority.Medium.id)))
  }

  val forwardingManager = new ForwardingManager(nameServer.multiTableForwarder[Shard])

  jobCodec += ("single.Single".r, new jobs.single.SingleJobParser(forwardingManager, OrderedUuidGenerator))
  jobCodec += ("multi.Multi".r, new jobs.multi.MultiJobParser(forwardingManager, jobScheduler, config.aggregateJobsPageSize))

  jobCodec += ("jobs\\.(Copy|Migrate)".r, new jobs.CopyParser(nameServer, jobScheduler(Priority.Medium.id)))

  val flockService = {
    val prefs = new PreferenceService(
      forwardingManager,
      jobScheduler,
      config.readFuture("readFuture"),
      config.aggregateJobsPageSize)

    new PrefStoreThriftAdapter(prefs, jobScheduler)
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

class PrefStoreThriftAdapter(val prefz: PreferenceService, val scheduler: PrioritizingJobScheduler) extends thrift.PreferenceService.Iface {
  import java.util.{ List => JList }
  import scala.collection.JavaConversions._
  import com.twitter.gizzard.thrift.conversions.Sequences._
  import me.huiwen.prefz.conversions.Preference._
  import me.huiwen.prefz.conversions.Page._
  import me.huiwen.prefz.conversions.Results._
  import com.twitter.gizzard.shards.ShardException
  import thrift.PrefException

  def create(graphId: Int, userId: Long, itemId: Long, source: String, action: String, createDate: Int, score: Double,
    status: Status, createType: CreateType) {
    prefz.create(graphId, userId, itemId, source, action, score, createDate, status, createType);
  }

  def createPreference(graphId: Int, pref: thrift.Preference) {
    prefz.createPreference(graphId, pref.fromThrift);
  }

  def delete(graphId: Int, userId: Long, itemId: Long, source: String, action: String) {
    prefz.delete(graphId, userId, itemId, source, action);
  }

  def deletePreference(graphId: Int, pref: thrift.Preference) {
    prefz.deletePreference(graphId, pref.fromThrift);
  }

  def update(graphId: Int, userId: Long, itemId: Long, source: String, action: String, score: Double, createDateInSeconds: Int,
    status: Status, createType: CreateType) {
    prefz.update(graphId, userId, itemId, source, action, score, Time(createDateInSeconds), status, createType);

  }

  def updatePreference(graphId: Int, pref: thrift.Preference) {
    prefz.updatePreference(graphId, pref.fromThrift);
  }

  def selectByUserItemSourceAndAction(graphId: Int, userId: Long, itemId: Long, source: String, action: String) {
    prefz.selectByUserItemSourceAndAction(graphId, userId, itemId, source, action).toThrift
  }
  def selectByUserSourceAndAction(userId: Long, source: String, action: String) {
    prefz.selectByUserSourceAndAction(userId, source, action)
  }
  def selectPageByUserSourceAndAction(graphId: Int, userId: Long, source: String, action: String, cursor: Cursor, count: Int) {
    prefz.selectPageByUserSourceAndAction(graphId, userId, source, action, cursor, count)
  }

  def selectBySourcAndAction(graphId: Int,source: String, action: String) {
    prefz.selectBySourcAndAction(graphId,source, action).map { _.toThrift }

  }
  def selectBySourcAndAction(graphId: Int, source: String, action: String, cursor: (Cursor, Cursor), count: Int) {
    prefz.selectPageBySourcAndAction(graphId, source, action, cursor, count)

  }

  def selectUserIdsBySource(source: String) {
    prefz.selectUserIdsBySource(source)
  }
  
  def selectByUser(userId: Long){
   
  }
   
  def selectPageByUser(userId: Long, cursor: Cursor, count: Int)
  {
    
  }
  def selectAll():Seq[Preference]
  {
  }
 def selectAllPage(cursor: (Cursor, Cursor), count: Int){
   
 }

}

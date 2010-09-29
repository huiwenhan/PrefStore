package com.ibm.webahead.steer.gizzard.preference

import net.lag.configgy.Configgy
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.database.{ApachePoolingDatabaseFactory, MemoizingDatabaseFactory}
import com.twitter.querulous.evaluator.StandardQueryEvaluatorFactory
import com.twitter.xrayspecs.TimeConversions._
import com.twitter.ostrich.W3CStats
import net.lag.logging.Logger


object Database {
  val log = Logger.get
  val conf = Configgy.config
  val w3c = new W3CStats(log, conf.getList("prefz.w3c").toArray)
  val databaseFactory = new MemoizingDatabaseFactory(new ApachePoolingDatabaseFactory(
    conf("prefz.db.connection_pool.size_min").toInt,
    conf("prefz.db.connection_pool.size_max").toInt,
    conf("prefz.db.connection_pool.test_idle_msec").toLong.millis,
    conf("prefz.db.connection_pool.max_wait").toLong.millis,
    conf("prefz.db.connection_pool.test_on_borrow").toBoolean,
    conf("prefz.db.connection_pool.min_evictable_idle_msec").toLong.millis))
  val queryEvaluatorFactory = new StandardQueryEvaluatorFactory(databaseFactory, new SqlQueryFactory)

  conf.getList("prefz.nameserver.hostnames").foreach { hostname =>
    val queryEvaluator = queryEvaluatorFactory(hostname, null, conf("prefz.nameserver.username"), conf("prefz.nameserver.password"))
    queryEvaluator.execute("DROP DATABASE IF EXISTS " + conf("prefz.nameserver.name"))
    queryEvaluator.execute("CREATE DATABASE " + conf("prefz.nameserver.name"))
  }
}

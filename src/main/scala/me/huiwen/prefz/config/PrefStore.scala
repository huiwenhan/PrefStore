package me.huiwen.prefz.config

import com.twitter.gizzard.config._
import com.twitter.ostrich.admin.config.AdminServiceConfig
import com.twitter.querulous.config.{Connection, QueryEvaluator}
import com.twitter.util.TimeConversions._
/*
import me.huiwen.prefz.queries.QueryTree
import me.huiwen.prefz.queries
*/

trait PrefStoreServer extends TServer {
  var name = "pref_store"
  var port = 7915
}

/*
trait IntersectionQuery {
  var intersectionTimeout = 100.millis
  var averageIntersectionProportion = 0.1
  var intersectionPageSizeMax = 4000

  def intersect(query1: QueryTree, query2: QueryTree) = new queries.IntersectionQuery(query1, query2, averageIntersectionProportion, intersectionPageSizeMax, intersectionTimeout)
  def difference(query1: QueryTree, query2: QueryTree) = new queries.DifferenceQuery(query1, query2, averageIntersectionProportion, intersectionPageSizeMax, intersectionTimeout)
}
*/
trait PrefStore extends GizzardServer {
  def server: PrefStoreServer

  //var intersectionQuery: IntersectionQuery = new IntersectionQuery { }
  var aggregateJobsPageSize = 500

  def databaseConnection: Connection

  def edgesQueryEvaluator: QueryEvaluator
  def lowLatencyQueryEvaluator: QueryEvaluator
  def materializingQueryEvaluator: QueryEvaluator

  def readFuture: Future

  def adminConfig: AdminServiceConfig
}
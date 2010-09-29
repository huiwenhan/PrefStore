package com.ibm.webahead.steer.gizzard.preference

import scala.collection.mutable
import com.twitter.results.{Cursor, ResultWindow}
import java.sql.{ SQLIntegrityConstraintViolationException, ResultSet }
import com.twitter.querulous.evaluator.{ QueryEvaluatorFactory, QueryEvaluator }
import net.lag.configgy.ConfigMap
import com.twitter.gizzard.shards
import com.twitter.querulous.query.SqlQueryTimeoutException
import java.sql.SQLException
import com.twitter.gizzard.proxy.SqlExceptionWrappingProxy
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
//import Shard.Cursor

class SqlShardFactory(queryEvaluatorFactory: QueryEvaluatorFactory, config: ConfigMap)
  extends shards.ShardFactory[Shard] {

  val TABLE_DDL = """
CREATE TABLE IF NOT EXISTS %s (
  userid               BIGINT                   NOT NULL,
  itemid               BIGINT                   NOT NULL,
  score                 FLOAT                    NOT NULL,
  action                VARCHAR(36)              NOT NULL,
  source                VARCHAR(36)              NOT NULL,
  createdate           INT UNSIGNED             NOT NULL,
  createtype      		INT 				 NOT NULL DEFAULT 0,
  PRIMARY KEY (id)
) TYPE=INNODB"""

  def instantiate(shardInfo: shards.ShardInfo, weight: Int, children: Seq[Shard]) = {
    val queryEvaluator = queryEvaluatorFactory(List(shardInfo.hostname), config("prefz.db.name"), config("prefz.db.username"), config("prefz.db.password"))
    SqlExceptionWrappingProxy[Shard](new SqlShard(queryEvaluator, shardInfo, weight, children))
  }

  def materialize(shardInfo: shards.ShardInfo) = {
    try {
      val queryEvaluator = queryEvaluatorFactory(
        List(shardInfo.hostname),
        config("prefz.db.name"),
        config("prefz.db.username"),
        config("prefz.db.password"))
      queryEvaluatorFactory(shardInfo.hostname, null, config("prefz.db.username"), config("prefz.db.password")).execute("CREATE DATABASE IF NOT EXISTS " + config("prefz.db.name"))
      queryEvaluator.execute(TABLE_DDL.format(shardInfo.tablePrefix + "_prefz"))
    } catch {
      case e: SQLException => throw new shards.ShardException(e.toString)
      case e: SqlQueryTimeoutException => throw new shards.ShardTimeoutException
    }
  }
}

class SqlShard(private val queryEvaluator: QueryEvaluator, val shardInfo: shards.ShardInfo,
  val weight: Int, val children: Seq[Shard]) extends Shard {

  private val table = shardInfo.tablePrefix + "_prefz"

  def create(userId: Long,
    itemId: Long, score: Double,
    source: String, action: String,
    createDate: Int,
    createType: Int) =
    write(new Preference(userId,
      itemId, score,
      source, action,
      createDate, createType))
  def destroy(pref: Preference) =
    remove(new Preference( pref.userId,
      pref.itemId, pref.score,
      pref.source, pref.action,
      pref.createDate, pref.createType))

  def read(userId: Long) = {
    val prefs = new mutable.ArrayBuffer[Preference]
    val rows = queryEvaluator.select("SELECT * FROM " + table + " where user_id = ? " , userId)
    { row =>
      prefs += makePreference(row)
    }
    prefs
  }

  
  def selectAll(cursor: Cursor, count: Int): (Seq[Preference], Option[Cursor]) = {
    val rows = queryEvaluator.select("SELECT * FROM " + table + "  LIMIT ?," + count + 1+"order by user_id", cursor)(makePreference(_))
    val chomped = rows.take(count)
    val nextCursor = if (chomped.size < rows.size) Some(Cursor(chomped.last.userId)) else None
    (chomped, nextCursor)
  }
  
//  def selectAll(cursor: Cursor, count: Int) = {
//	
//	val prefs = new mutable.ArrayBuffer[(Preference)]
//    val rows = queryEvaluator.select("SELECT * FROM " + table + "  LIMIT ?," + count + 1, cursor)
//    { row =>
//      prefs += (makePreference(row))
//    }
//	prefs
 //   var page = prefs.projection
 //   if (cursor < Cursor.Start) page = page.reverse
 //   new ResultWindow(page, count, cursor)
  //}
  def selectPreferencesBySourcAndAction(source: String, action: String): Seq[Preference] =
  {
      val rows = queryEvaluator.select("SELECT * FROM " + table + " WHERE source=? and action= ?  ", source, action)(makePreference(_))
      rows
  }
  def write(preferences: Seq[Preference]) = preferences.foreach(write(_))

  def write(pref: Preference) = {
    val Preference(userId, itemId, score, source, action, createDate, createType) = pref

    queryEvaluator.execute("INSERT INTO " + table + " (userid, itemid, score, source,action,createdate,createtype) VALUES ( ?, ?, ?, ?,?,?,?)",
       userId, itemId, score, source, action, createDate, createType)

  }

  def remove(pref: Preference) = {
    val Preference(userId, itemId, score, source, action, create, createType) = pref

    queryEvaluator.execute("DELETE FROM " + table + "  WHERE user_id = ? and item_id= ? and source =? and action =?", 
    		userId,itemId,source,action)

  }

  private def insertOrUpdate(f: => Unit)(g: => Unit) {
    try {
      f
    } catch {
      case e: SQLIntegrityConstraintViolationException => g
    }
  }
  /*
  private def makeRow(row: ResultSet) = {
    new Row(row.getLong("id"), row.getString("name"), Time(row.getLong("created_at").seconds), Time(row.getLong("updated_at").seconds), State(row.getInt("state")))
  }
  */

  private def makePreference(pref: ResultSet) = {
    new Preference(
      pref.getLong("userid"),
      pref.getLong("itemid"),
      pref.getInt("score"),
      pref.getString("source"),
      pref.getString("action"),
      pref.getInt("createdate"),
      pref.getInt("createtype")
      )
  }
}
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
package shards

import java.util.Random
import java.sql.{ BatchUpdateException, ResultSet, SQLException, SQLIntegrityConstraintViolationException }
import scala.collection.mutable
import com.twitter.gizzard.proxy.SqlExceptionWrappingProxyFactory
import com.twitter.gizzard.Stats
import com.twitter.gizzard.shards._
import com.twitter.querulous.config.Connection
import com.twitter.querulous.evaluator.{ QueryEvaluator, QueryEvaluatorFactory, Transaction }
import com.twitter.querulous.query
import com.twitter.querulous.query.{ QueryClass => QuerulousQueryClass, SqlQueryTimeoutException }
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import com.mysql.jdbc.exceptions.MySQLTransactionRollbackException
import me.huiwen.prefz._
import thrift.PrefException

object QueryClass {
  val Select = QuerulousQueryClass.Select
  val Execute = QuerulousQueryClass.Execute
  val SelectSingle = QuerulousQueryClass("select_single")
  val SelectModify = QuerulousQueryClass("select_modify")
  val SelectCopy = QuerulousQueryClass("select_copy")
}

class SqlShardFactory(
  instantiatingQueryEvaluatorFactory: QueryEvaluatorFactory,
  lowLatencyQueryEvaluatorFactory: QueryEvaluatorFactory,
  materializingQueryEvaluatorFactory: QueryEvaluatorFactory,
  connection: Connection)
  extends ShardFactory[Shard] {

  val deadlockRetries = 3

  val PREFERENCE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS %s (
user_id BIGINT NOT NULL,
item_id BIGINT NOT NULL,
source  VARCHAR(36) NOT NULL,
action  VARCHAR(36) NOT NULL,
score  FLOAT NOT NULL,
created INT UNSIGNED NOT NULL,
create_type TINYINT UNSIGNED NOT NULL,
status TINYINT NOT NULL,

PRIMARY KEY (user_id, item_id, source,action)

) ENGINE=INNODB"""

  def instantiate(shardInfo: ShardInfo, weight: Int) = {
    val queryEvaluator = instantiatingQueryEvaluatorFactory(connection.withHost(shardInfo.hostname))
    val lowLatencyQueryEvaluator = lowLatencyQueryEvaluatorFactory(connection.withHost(shardInfo.hostname))
    new SqlExceptionWrappingProxyFactory[Shard](shardInfo.id).apply(
      new SqlShard(shardInfo, queryEvaluator, lowLatencyQueryEvaluator, deadlockRetries))
  }

  //XXX: enforce readonly connection
  def instantiateReadOnly(shardInfo: ShardInfo, weight: Int) = instantiate(shardInfo, weight)

  def materialize(shardInfo: ShardInfo) = {
    try {
      val queryEvaluator = materializingQueryEvaluatorFactory(connection.withHost(shardInfo.hostname).withoutDatabase)

      queryEvaluator.execute("CREATE DATABASE IF NOT EXISTS " + connection.database)
      queryEvaluator.execute(PREFERENCE_TABLE_DDL.format(connection.database + "." + shardInfo.tablePrefix + "_edges", shardInfo.sourceType, shardInfo.destinationType))
    } catch {
      case e: SQLException => throw new ShardException(e.toString)
      case e: SqlQueryTimeoutException => throw new ShardTimeoutException(e.timeout, shardInfo.id, e)
    }
  }
}

class SqlShard(
  shardInfo: ShardInfo,
  queryEvaluator: QueryEvaluator,
  lowLatencyQueryEvaluator: QueryEvaluator,
  deadlockRetries: Int)
  extends Shard {

  private val tablePrefix = shardInfo.tablePrefix
  private val randomGenerator = new Random

  import QueryClass._
  def addPreference(pref: Preference)
  {
    
  }
  
  def add(userId: Long, itemId: Long, source: String, action: String, score: Double,updatedAt: Time, 
    status: Status, createType: CreateType)
  {
    
  }
  
  def delete(userId: Long, itemId: Long, source: String, action: String)
  {
    
  }
  
  def deletePreference(pref: Preference)
  {
    
  }
  
   def update(userId: Long, itemId: Long, source: String, action: String,  score: Double,updatedAt: Time,
    status: Status, createType: CreateType)
   {
     
   }
   
   def updatePreference(pref: Preference)
   {
     
   }
   
   def writeCopies(Preference: Seq[Preference])
   {
     
   }
  def selectByUserItemSourceAndAction(
    userId: Long, itemId: Long, source: String, action: String) :Option[Preference]=
    {
      lowLatencyQueryEvaluator.selectOne(SelectSingle, "SELECT * FROM " 
          + tablePrefix + "_preference WHERE user_id = ? And item_id = ? AND source = ? and action = ?", userId, itemId, source, action) { row =>
        makePreference(row)
      }
     
    }
  
  def selectByUserSourceAndAction(userId: Long, source: String, action: String): Seq[Preference]=
  {
     val prefs = new mutable.ArrayBuffer[Preference]
    lowLatencyQueryEvaluator.select(SelectSingle, "SELECT * FROM " + tablePrefix + "_preference WHERE user_id=? and source = ? and action = ?", userId,source, action)
    { row =>
      prefs+=makePreference(row)
    }
    prefs
  }
  def selectPageByUserSourceAndAction(userId: Long, source: String, action: String,cursor:Cursor,count:Int):(Seq[Preference],Cursor) = {
   val prefs = new mutable.ArrayBuffer[Preference]
	var nextCursor  = Cursor.Start
	var retCursor = Cursor.End
	var i=0
	val query =   "SELECT * FROM " + tablePrefix + "_preference  Use INDEX (idx_userid_item_id) WHERE  user_id =? and source = ? and action = ? and  item_id >?  order by item_id limit ?"
    queryEvaluator.select(SelectCopy ,query,userId,source, action,cursor.position,count+1) 
    { row =>
     if(i<count)
     {
       
       prefs+=makePreference(row)
       nextCursor =Cursor(row.getLong("item_id"))
       i+1    
     }else
     {
       retCursor = nextCursor;
     }
    }
	(prefs,retCursor)
  }

  def selectBySourcAndAction(source: String, action: String) :Seq[Preference]= {
    val prefs = new mutable.ArrayBuffer[Preference]
    lowLatencyQueryEvaluator.select(SelectSingle, "SELECT * FROM " + tablePrefix + "_preference WHERE  source = ? and action = ?", source, action) { row =>
      prefs+=makePreference(row)
    }
    prefs
  }

 def selectPageBySourcAndAction(source: String, action: String,cursor:(Cursor,Cursor),count:Int) 
  	:(Seq[Preference],(Cursor,Cursor))= {
	val prefs = new mutable.ArrayBuffer[Preference]
	var nextCursor  = (Cursor.Start,Cursor.End)
	var retCursor = (Cursor.End,Cursor.End)
	var i=0
	val query =   "SELECT * FROM " + tablePrefix + "_preference  Use INDEX (idx_userid_item_id) WHERE  source = ? and action = ? and ((user_id = ? and item_id >? ) Or (user_id> ?)) order by user_id ,item_id limit ?"
	val (cursor1,cursor2) = cursor
    queryEvaluator.select(SelectCopy ,query,source, action,cursor1.position,cursor2.position,cursor1.position,count+1) 
    { row =>
     if(i<count)
     {
       
       prefs+=makePreference(row)
       nextCursor =(Cursor(row.getLong("user_id")),Cursor(row.getLong("item_id")))
       i+1    
     }else
     {
       retCursor = nextCursor;
     }
    }
	(prefs,retCursor)
  }
  def selectUserIdsBySource(source: String) =
    {
	  throw new PrefException("")
    }

  def selectByUser(userId: Long):Seq[Preference]=
  {
    throw new PrefException("")
  }
  def selectPageByUser(userId: Long, cursor: Cursor, count: Int):ResultWindow[Preference]=
  {
    val prefs = new mutable.ArrayBuffer[(Preference,Cursor)]
	val query =   "SELECT * FROM " + tablePrefix + "_preference  Use INDEX (idx_userid_item_id) WHERE  user_id =?  and  item_id >?  order by item_id limit ?"
    queryEvaluator.select(SelectCopy ,query,userId,cursor.position,count+1) 
    { row =>  
       prefs+=(makePreference(row)->Cursor(row.getLong("item_id")))
    }
    var page=prefs.view
    if(cursor< Cursor.Start) page=page.reverse
	new ResultWindow(page,count,cursor)
  }
  
  
  def selectAll():Seq[Preference]=
  {
    throw new PrefException("")
  }
  def selectAllPage(cursor: (Cursor, Cursor), count: Int):(Seq[Preference], (Cursor,Cursor))=
  {
 	val prefs = new mutable.ArrayBuffer[Preference]
	var nextCursor  = (Cursor.Start,Cursor.End)
	var retCursor = (Cursor.End,Cursor.End)
	var i=0
	val query =   "SELECT * FROM " + tablePrefix + "_preference  Use INDEX (idx_userid_item_id) WHERE   ((user_id = ? and item_id >? ) Or (user_id> ?)) order by user_id ,item_id limit ?"
	val (cursor1,cursor2) = cursor
    queryEvaluator.select(SelectCopy ,query,cursor1.position,cursor2.position,cursor1.position,count+1) 
    { row =>
     if(i<count)
     {
       
       prefs+=makePreference(row)
       nextCursor =(Cursor(row.getLong("user_id")),Cursor(row.getLong("item_id")))
       i+1    
     }else
     {
       retCursor = nextCursor;
     }
    }
	(prefs,retCursor)
  }
    
  private def opposite(direction: String) = direction match {
    case "ASC" => "DESC"
    case "DESC" => "ASC"
    case "<" => ">="
    case ">" => "<="
  }

  override def equals(other: Any) = {
    error("called!")
    false
  }

  override def hashCode = {
    (if (tablePrefix == null) 37 else tablePrefix.hashCode * 37) + (if (queryEvaluator == null) 1 else queryEvaluator.hashCode)
  }

  private def insertPreference(transaction: Transaction, pref: Preference): Int = {
    val insertedRows =
      transaction.execute("INSERT INTO " + tablePrefix + "_preference (user_id, item_id,source" +
        "action, created,score,status ,create_Type" +
        "VALUES (?, ?, ?, ?, ?, ?,?,?)",
        pref.userId, pref.itemId, pref.source, pref.action, pref.updatedAt.inSeconds,
        pref.score, pref.status.id, pref.createType.id)
    insertedRows
  }

  def bulkUnsafeInsertPreferences(prefs: Seq[Preference]) {
    bulkUnsafeInsertPreferences(queryEvaluator, prefs)
  }

  def bulkUnsafeInsertPreferences(transaction: QueryEvaluator, prefs: Seq[Preference]) = {
    if (prefs.size > 0) {
      val query = "INSERT INTO " + tablePrefix + "_preference (user_id, item_id,source,action, created,score,status ,create_Type VALUES (?, ?, ?, ?, ?, ?,?,?)"
      transaction.executeBatch(query) { batch =>
        prefs.foreach { pref =>
          batch(pref.userId, pref.itemId, pref.source, pref.action, pref.updatedAt.inSeconds,
            pref.score, pref.status.id, pref.createType.id)
        }
      }
    }
  }

  private def updatePreference(transaction: Transaction, pref: Preference,
    oldPref: Preference): Int = {
    if ((oldPref.createDateSeconds == pref.createDateSeconds)) return 0

    try {
      transaction.execute("UPDATE " + tablePrefix + "_preference SET updated_at = ?, " +
        "score = ?, status = ?, create_type= ? " +
        "WHERE user_id = ? AND item_id = ? AND source =  ? AND action = ?",
        pref.updatedAt.inSeconds, pref.score, pref.status.id, pref.createType.id,
        pref.userId, pref.itemId, pref.source, pref.action)
    } catch {
      case e: SQLIntegrityConstraintViolationException =>
        // usually this is a (source_id, state, position) violation. scramble the position more.
        // FIXME: hacky. remove with the new schema.
        transaction.execute("UPDATE " + tablePrefix + "_preference SET updated_at = ?, " +
          "score = ?, status = ?, create_type= ? " +
          "WHERE user_id = ? AND item_id = ? AND source =  ? AND action = ?",
          pref.updatedAt.inSeconds, pref.score, pref.status.id, pref.createType.id,
          pref.userId, pref.itemId, pref.source, pref.action)
    }

    0
  }

  // returns +1, 0, or -1, depending on how the metadata count should change after this operation.
  // `predictExistence`=true for normal operations, false for copy/migrate.

  private def writePreference(transaction: Transaction, pref: Preference,
    predictExistence: Boolean): Int = {
    val countDelta = if (predictExistence) {
      transaction.selectOne(SelectModify,
        "SELECT * FROM " + tablePrefix + "_preferences WHERE user_id = ? " +
          "and item_id = ? and source = ? and action = ?", pref.userId, pref.itemId, pref.source, pref.action) { row =>
          makePreference(row)
        }.map { oldRow =>
          updatePreference(transaction, pref, oldRow)
        }.getOrElse {
          insertPreference(transaction, pref)
        }
    } else {
      try {
        insertPreference(transaction, pref)
      } catch {
        case e: SQLIntegrityConstraintViolationException =>
          transaction.selectOne(SelectModify,
            "SELECT * FROM " + tablePrefix + "_preferences WHERE user_id = ? " +
              "and item_id = ? and source = ? and action = ?", pref.userId, pref.itemId, pref.source, pref.action) { row =>
              makePreference(row)
            }.map { oldRow =>
              updatePreference(transaction, pref, oldRow)
            }.getOrElse(0)
      }
    }
    countDelta
  }

  private def write(transaction: Transaction, pref: Preference) {
    write(transaction, pref, deadlockRetries, true)
  }

  private def write(transaction: Transaction, pref: Preference, tries: Int, predictExistence: Boolean) {
    try {

      val countDelta = writePreference(transaction, pref, predictExistence)

    } catch {
      case e: MySQLTransactionRollbackException if (tries > 0) =>
        write(transaction, pref, tries - 1, predictExistence)
      case e: SQLIntegrityConstraintViolationException if (tries > 0) =>
        // temporary. until the position differential between master/slave is fixed, it's
        // possible for a slave migration to have two different edges with the same position.
        write(transaction, new Preference(pref.userId, pref.itemId, pref.source, pref.action, pref.updatedAt,
          pref.score, pref.status, pref.createType), tries - 1, predictExistence)
    }
  }

  /**
   * Mysql may throw an exception for a bulk operation that actually partially completed.
   * If that happens, try to pick up the pieces and indicate what happened.
   */
  case class BurstResult(completed: Seq[Preference], failed: Seq[Preference])

  def writeBurst(transaction: Transaction, preferences: Seq[Preference]): BurstResult = {
    try {
      bulkUnsafeInsertPreferences(transaction, preferences)
      BurstResult(preferences, Nil)
    } catch {
      case e: BatchUpdateException =>
        val completed = new mutable.ArrayBuffer[Preference]
        val failed = new mutable.ArrayBuffer[Preference]
        e.getUpdateCounts.zip(preferences).foreach {
          case (errorCode, edge) =>
            if (errorCode < 0) {
              failed += edge
            } else {
              completed += edge
            }
        }
        BurstResult(completed, failed)
    }
  }

  def writeCopies(transaction: Transaction, preferences: Seq[Preference]) {
    if (!preferences.isEmpty) {
      Stats.addMetric("copy-burst", preferences.size)

      var userIdsSet = Set[Long]()
      preferences.foreach { preference => userIdsSet += preference.userId }
      val userIds = userIdsSet.toSeq

      val result = writeBurst(transaction, preferences)
      if (result.completed.size > 0) {
        var currentSourceId = -1L
        var countDeltas = new Array[Int](4)
        result.completed.foreach { preference =>
          if (preference.userId != currentSourceId) {
            if (currentSourceId != -1)
              //updateCount(transaction, currentSourceId, countDeltas(metadataById(currentSourceId).state.id))
              currentSourceId = preference.userId
            countDeltas = new Array[Int](4)
          }
          countDeltas(preference.status.id) += 1
        }
        // updateCount(transaction, currentSourceId,countDeltas(metadataById(currentSourceId).state.id))
      }

      if (result.failed.size > 0) {
        Stats.incr("copy-fallback")
        var currentSourceId = -1L
        var countDelta = 0
        result.failed.foreach { preference =>
          if (preference.userId != currentSourceId) {
            if (currentSourceId != -1)
              //updateCount(transaction, currentSourceId, countDelta)
              currentSourceId = preference.userId
            countDelta = 0
          }
          countDelta += writePreference(transaction, preference, false)
        }
        //updateCount(transaction, currentSourceId, countDelta)
      }

    }
  }

  private def makePreference(userId: Long, itemId: Long, source: String, action: String,
    updatedAt: Time, score: Double, status: Status, createType: CreateType): Preference = {
    new Preference(userId, itemId, source, action, updatedAt.inSeconds, score, status, createType)
  }

  private def makePreference(row: ResultSet): Preference = {
    makePreference(row.getLong("user_id"), row.getLong("item_id"),
      row.getString("source"), row.getString("action"), Time.fromSeconds(row.getInt("updated_at")),
      row.getDouble("score"), Status(row.getInt("status")), CreateType(row.getInt("createType")))
  }

}


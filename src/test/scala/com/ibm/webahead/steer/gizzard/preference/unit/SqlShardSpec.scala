package com.ibm.webahead.steer.gizzard.preference.unit

import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import com.twitter.gizzard.shards.{ShardInfo, Busy,ShardId}
import com.twitter.results.{Cursor, ResultWindow}

object SqlShard extends ConfiguredSpecification with JMocker with ClassMocker {
  "SqlShard" should {
    import Database._
    Time.freeze()

    val shardFactory = new SqlShardFactory(queryEvaluatorFactory, config)
    val shardId =new ShardId("localhost","table_001")
    val shardInfo = new ShardInfo(shardId,"com.twitter.service.flock.edges.SqlShard",
       "", "", Busy.Normal)
    val sqlShard = shardFactory.instantiate(shardInfo, 1, List[Shard]())
    val row = new Preference(1,1,1,"TAP","computed", 1, 1)
    val row2 = new Preference(2,1,1,"TAP","computed", 1, 1)
    val queryEvaluator = queryEvaluatorFactory("localhost", null, config("prefz.db.username"), config("prefz.db.password"))

    doBefore {
      queryEvaluator.execute("DROP DATABASE IF EXISTS " + config("prefz.db.name"))
      shardFactory.materialize(shardInfo)
    }

    "create & read" in {
      sqlShard.create(row.userId,row.itemId,row.score,row.source,row.action, row.createDate, row.createType)
      sqlShard.read(row.userId) mustEqual Some(row)
      sqlShard.destroy(row)
    }

    "create, destroy then read" in {
      sqlShard.create(row.userId,row.itemId,row.score,row.source,row.action, row.createDate, row.createType)
      sqlShard.destroy(row)
      sqlShard.read(row.userId) mustEqual None
    }

    "idempotent" in {
      "read a nonexistent row" in {
        sqlShard.read(row.userId) mustEqual None
      }

      "destroy, create, then read" in {
        sqlShard.destroy(row)
        sqlShard.create(row.userId,row.itemId,row.score,row.source,row.action, row.createDate, row.createType)
        sqlShard.read(row.userId) mustEqual Some(row)
      }
    }

    "selectAll" in {
      doBefore {
        sqlShard.create(row.userId,row.itemId,row.score,row.source,row.action, row.createDate, row.createType)
        sqlShard.create(row2.userId,row2.itemId,row2.score,row2.source,row2.action, row2.createDate, row2.createType)
      }

      "start cursor" in {
        val (rows, nextCursor) = sqlShard.selectAll(Cursor.Start , 1)
        rows.toList mustEqual List(row)
        nextCursor mustEqual Some(row.userId)
      }

      "multiple items" in {
        val (rows, nextCursor) = sqlShard.selectAll(Cursor.Start, 2)
        rows.toList mustEqual List(row, row2)
        nextCursor mustEqual None
      }

      "non-start cursor" in {
        val (rows, nextCursor) = sqlShard.selectAll(Cursor.Start, 1)
        rows.toList mustEqual List(row2)
        nextCursor mustEqual None
      }
    }
  }
}


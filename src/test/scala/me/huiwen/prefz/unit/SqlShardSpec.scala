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
package unit

import java.sql.SQLException
import scala.collection.JavaConversions._
import scala.collection.mutable
import com.twitter.gizzard.shards.{ Busy, ShardId, ShardInfo }
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.querulous.evaluator.{ QueryEvaluator, QueryEvaluatorFactory, StandardQueryEvaluatorFactory, Transaction }
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import org.specs.mock.JMocker
import conversions.Preference._
import conversions.PrefResults._
import conversions.Results._
import shards.{ Shard, SqlShard, SqlShardFactory }
import thrift.{ Results, PrefResults }

class SqlShardSpec extends IntegrationSpecification with JMocker {
  "Pref SqlShard" should {
    val user1 = 1L
    val user2 = 2L
    val user3 = 3L
    val user4 = 4L
    val user5 = 5L
    val user6 = 6L

    val item1 = 1L
    val item2 = 2L
    val item3 = 3L
    val item4 = 4L
    val item5 = 5L
    val item6 = 6L

    val source1 = "media"
    val source2 = "learning"

    val action1 = "click"
    val action2 = "download"

    val score1 = 1.0f
    val score2 = 2.0f

    val createTypeDefault = CreateType.DEFAULT
    val createTypeThumb = CreateType.THUMB

    val statusValid = Status.VALID
    val statusIgnore = Status.IGNORE

    val now = Time.now

    val queryEvaluatorFactory = config.edgesQueryEvaluator()
    val queryEvaluator = queryEvaluatorFactory(config.databaseConnection)
    val shardFactory = new SqlShardFactory(queryEvaluatorFactory, queryEvaluatorFactory, queryEvaluatorFactory, config.databaseConnection)
    val shardInfo = ShardInfo(ShardId("localhost", "table_001"), "me.huiwen.prefz.shard.SqlShard",
      "INT UNSIGNED", "INT UNSIGNED", Busy.Normal)
    var shard: Shard = null

    doBefore {
      try {
        reset(config, config.databaseConnection.database)
        shardFactory.materialize(shardInfo)
        shard = shardFactory.instantiate(shardInfo, 1)
      } catch { case e => e.printStackTrace() }
    }

    "create" in {
      val createShardFactory = new SqlShardFactory(queryEvaluatorFactory, queryEvaluatorFactory, queryEvaluatorFactory, config.databaseConnection)
      val createShardInfo = ShardInfo(ShardId("localhost", "create_test"), "me.huiwen.prefz.shard.SqlShard",
        "INT UNSIGNED", "INT UNSIGNED", Busy.Normal)
      val createShard = new SqlShard(createShardInfo, queryEvaluator, queryEvaluator, 0)

      "when the database doesn't exist" >> {
        createShardFactory.materialize(createShardInfo)
        queryEvaluator.select("SELECT * FROM create_test_prefernce") { row => row }.isEmpty mustBe true
      }

      "when the database does exist but the table doesn't exist" >> {
        createShardFactory.materialize(createShardInfo)
        queryEvaluator.select("SELECT * FROM create_test_prefernce") { row => row }.isEmpty mustBe true
      }

      "when both the database and table already exist" >> {
        createShardFactory.materialize(createShardInfo)
        createShardFactory.materialize(createShardInfo)
        queryEvaluator.select("SELECT * FROM create_test_prefernce") { row => row }.isEmpty mustBe true
      }
    }

    "selectBySourcAndAction" in {
      "all at once" >> {
        shard.add(user1, item1, source1, action1, score1, now, statusValid, createTypeDefault)
        shard.add(user2, item2, source1, action1, score1, now, statusValid, createTypeDefault)

        val rows = new mutable.ArrayBuffer[Preference]
        rows ++= List(shard.selectByUserItemSourceAndAction(user1, item1, source1, action1).get(), shard.selectByUserItemSourceAndAction(user2, item2, source1, action1).get())
        shard.selectBySourcAndAction(source1, action1) mustEqual (rows, (Cursor.End, Cursor.End))
      }

    }

  }
}

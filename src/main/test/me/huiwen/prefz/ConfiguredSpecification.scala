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

import java.io.File
import org.specs.Specification
import com.twitter.gizzard.shards.{Busy, ShardId, ShardInfo}
import com.twitter.gizzard.nameserver.Forwarding
import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.test.NameServerDatabase
import com.twitter.util.Eval
import com.twitter.querulous.evaluator.QueryEvaluatorFactory
import com.twitter.logging.Logger
import scala.collection.mutable
import me.huiwen.prefz

object MemoizedQueryEvaluators {
  val evaluators = mutable.Map[String,QueryEvaluatorFactory]()
}

abstract class ConfiguredSpecification extends Specification {
  lazy val config = {
    val c = Eval[prefz.config.PrefStore](new File("config/test.scala"))
    try {
      c.loggers.foreach { _() }
      c
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
  }

  def jsonMatching(list1: Iterable[JsonJob], list2: Iterable[JsonJob]) = {
    list1 must eventually(verify(l1 => { l1.map(_.toJson).sameElements(list2.map(_.toJson))}))
  }
}

abstract class IntegrationSpecification extends ConfiguredSpecification with NameServerDatabase {
  lazy val prefStore = {
    val f = new PrefStore(config)
    f.jobScheduler.start()
    f
  }

  lazy val flockService = prefStore.prefService

  def reset(config: prefz.config.PrefStore) { reset(config, 1) }

  def reset(config: prefz.config.PrefStore, count: Int) {
    materialize(config)
    prefStore.nameServer.reload()

    val rootQueryEvaluator = config.edgesQueryEvaluator()(config.databaseConnection.withoutDatabase)
    //rootQueryEvaluator.execute("DROP DATABASE IF EXISTS " + config.databaseConnection.database)
    val queryEvaluator = config.edgesQueryEvaluator()(config.databaseConnection)

    for (graph <- (1 until 3)) {
      Seq("forward", "backward").foreach { direction =>
        val tableId = if (direction == "forward") graph else graph * -1
        val replicatingShardId = ShardId("localhost", "replicating_" + direction + "_" + graph)
        prefStore.shardManager.createAndMaterializeShard(
          ShardInfo(replicatingShardId, "com.twitter.gizzard.shards.ReplicatingShard", "", "", Busy.Normal)
        )
        prefStore.shardManager.setForwarding(Forwarding(tableId, 0, replicatingShardId))

        for (sqlShardId <- (1 to count)) {
          val shardId = ShardId("localhost", direction + "_" + sqlShardId + "_" + graph)

          prefStore.shardManager.createAndMaterializeShard(ShardInfo(shardId,
            "com.twitter.flockdb.SqlShard", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal))
          prefStore.shardManager.addLink(replicatingShardId, shardId, 1)

          queryEvaluator.execute("DELETE FROM " + shardId.tablePrefix + "_edges")
          queryEvaluator.execute("DELETE FROM " + shardId.tablePrefix + "_metadata")
        }
      }
    }

    prefStore.nameServer.reload()
  }

  def jobSchedulerMustDrain = {
    var last = prefStore.jobScheduler.size
    while(prefStore.jobScheduler.size > 0) {
      prefStore.jobScheduler.size must eventually(be_<(last))
      last = prefStore.jobScheduler.size
    }
    while(prefStore.jobScheduler.activeThreads > 0) {
      Thread.sleep(10)
    }
  }

  def reset(config: prefz.config.PrefStore, db: String) {
    try {
      evaluator(config).execute("DROP DATABASE IF EXISTS " + db)
    } catch {
      case e =>
        e.printStackTrace()
        throw e
    }
  }
}

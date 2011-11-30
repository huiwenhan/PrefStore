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

import scala.collection.mutable
import com.twitter.gizzard.scheduler.{JsonJob, PrioritizingJobScheduler}
import com.twitter.gizzard.shards._
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import org.specs.mock.{ClassMocker, JMocker}
import me.huiwen.prefz
import prefz.{Status,CreateType,Priority,Cursor}
import shards.{Shard, SqlShard, ReadWriteShardAdapter}
import jobs.single.Single
import jobs.multi.Multi

class JobSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  
    val graphId1 =1;
    
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
    
    val priorityHigh = Priority.High
    
    val aggregateJobPageSize =2
    
    val cursor = Cursor(-1)

  val forwardingManager = mock[ForwardingManager]
  val mocks             = (0 to 3) map { _ => mock[Shard] }

  // allow the readwrite shard adapter to implement optimistically
  val shards    = mocks map { m => LeafRoutingNode(m) }
  val scheduler = mock[PrioritizingJobScheduler]

  "Single" should {
    "toJson" in {
      Time.withCurrentTimeFrozen { time =>
        val job = new Single(graphId1,user1,item1,source1,action1,score1,now,statusValid,createTypeDefault,forwardingManager)
        val json = job.toJson
        json mustMatch "Single"
        json mustMatch "\"graph_id\":" + graphId1
        json mustMatch "\"user_id\":" + user1
        json mustMatch "\"item_id\":" + item1
        json mustMatch "\"source\":" + source1
        json mustMatch "\"action\":" + action1
        json mustMatch "\"score\":" + score1
        json mustMatch "\"create_date\":" + Time.now.inSeconds
        json mustMatch "\"status\":" + statusValid.id
        json mustMatch "\"create_type\":" + createTypeDefault.id
      }
    }

  }

  "Multi" should {
    "toJson" in {
      Time.withCurrentTimeFrozen { time =>
        val job = new Multi(graphId1,user1,source1,action1,score1,now,statusValid,createTypeDefault,priorityHigh,aggregateJobPageSize,null,null)
        val json = job.toJson
        json mustMatch "Multi"
        json mustMatch "\"graph_id\":" + graphId1
        json mustMatch "\"user_id\":" + user1
        json mustMatch "\"source\":" + source1
        json mustMatch "\"action\":" + action1
        json mustMatch "\"score\":" + score1
        json mustMatch "\"create_date\":" + Time.now.inSeconds
        json mustMatch "\"status\":" + statusValid.id
        json mustMatch "\"create_type\":" + createTypeDefault.id
        json mustMatch "\"priority\":" + priorityHigh.id
        json mustMatch "\"create_type\":" + createTypeDefault.id
      }
    }
  }
}

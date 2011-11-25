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

package me.huiwen.prefz.jobs.multi

import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.shards.ShardBlackHoleException
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import me.huiwen.prefz.{ Status, ForwardingManager, Cursor, CreateType }
import me.huiwen.prefz.conversions.Numeric._
import me.huiwen.prefz.shards.Shard
import me.huiwen.prefz.jobs.single.Single
import me.huiwen.prefz.Preference
import me.huiwen.prefz.Priority

class MultiJobParser(
  forwardingManager: ForwardingManager,
  scheduler: PrioritizingJobScheduler,
  aggregateJobPageSize: Int)
  extends JsonJobParser {

  def apply(attributes: Map[String, Any]): JsonJob = {
    val casted = attributes.asInstanceOf[Map[String, AnyVal]]

    new Multi(
      casted("graph_id").toInt,
      casted("user_id").toLong,
      casted("item_id").toLong,
      casted("source").toString(),
      casted("action").toString(),
      casted("score").toDouble.toFloat,
      Time.fromSeconds(casted("create_date").toInt),
      Status(casted("status").toInt),
      CreateType(casted("create_type").toInt),
      Priority(casted.get("priority").map(_.toInt).getOrElse(Priority.Low.id)),
      aggregateJobPageSize,
      casted.get("cursor").map(c => Cursor(c.toLong)).getOrElse(Cursor.Start),
      forwardingManager,
      scheduler)
  }
}

class Multi(
  graphId: Int,
  userId: Long,
  itemId: Long,
  source: String,
  action: String,
  score: Float,
  createDate: Time,
  status: Status,
  createType: CreateType,
  priority: Priority.Value,
  aggregateJobPageSize: Int,
  var cursor: Cursor,
  forwardingManager: ForwardingManager,
  scheduler: PrioritizingJobScheduler)
  extends JsonJob {

  def this(
    graphId: Int,
    userId: Long,
    itemId: Long,
    source: String,
    action: String,
    score: Float,
    createDate: Time,
    status: Status,
    createType: CreateType,
    priority: Priority.Value,
    aggregateJobPageSize: Int,
    forwardingManager: ForwardingManager,
    scheduler: PrioritizingJobScheduler) = {
    this(
      graphId,
      userId,
      itemId,
      source,
      action,
      score,
      createDate,
      status,
      createType,
      priority,
      aggregateJobPageSize,
      Cursor.Start,
      forwardingManager,
      scheduler)
  }

  def toMap = Map(
    "graph_id" -> graphId,
    "user_id" -> userId,
    "item_id" -> itemId,
    "source" -> source,
    "action" -> action,
    "score" -> score,
    "create_date" -> createDate.inSeconds,
    "status" -> status.id,
    "create_type" -> createType.id,
    "cursor" -> cursor.position)

  def apply() {
    val forwardShard = forwardingManager.find(userId, graphId)

    while (cursor != Cursor.End) {
      val resultWindow = forwardShard.selectPageByUser(userId, cursor, aggregateJobPageSize)

      val chunkOfTasks = resultWindow.map { preference =>

        singlePrefJob(graphId, preference)
      }

      scheduler.put(priority.id, new JsonNestedJob(chunkOfTasks))

      // "commit" the current iteration by saving the next cursor.
      // if the job blows up in the next round, it will be re-serialized
      // with this cursor.
      cursor = resultWindow.nextCursor
    }
  }

  // XXX: since this job gets immediately serialized, pass null for forwardingManager and uuidGenerator.
  protected def singlePrefJob(graphId: Int, pref: Preference) = {
    new Single(graphId, pref.userId, pref.itemId, pref.source, pref.action, pref.score, pref.updatedAt, pref.status, pref.createType, null, null)
  }

  override def equals(o: Any) = o match {
    case o: Multi => this.toMap == o.toMap
    case _ => false
  }
}



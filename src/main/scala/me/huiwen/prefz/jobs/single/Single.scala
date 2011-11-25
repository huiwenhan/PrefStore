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

package me.huiwen.prefz.jobs.single

import com.twitter.logging.Logger
import com.twitter.util.{ Time, Return, Throw }
import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.shards._
import com.twitter.conversions.time._
import me.huiwen.prefz.Status
import me.huiwen.prefz
import me.huiwen.prefz.ForwardingManager
import me.huiwen.prefz.CreateType
import me.huiwen.prefz.UuidGenerator
import me.huiwen.prefz.conversions.Numeric._
import me.huiwen.prefz.shards.Shard

class SingleJobParser(
  forwardingManager: ForwardingManager,
  uuidGenerator: UuidGenerator)
  extends JsonJobParser {

  def log = Logger.get

  def apply(attributes: Map[String, Any]): JsonJob = {
    val writeSuccesses = try {
      attributes.get("write_successes") map {
        _.asInstanceOf[Seq[Seq[String]]] map { case Seq(h, tp) => ShardId(h, tp) }
      } getOrElse Nil
    } catch {
      case e => {
        log.warning("Error parsing write successes. falling back to non-memoization", e)
        Nil
      }
    }

    val casted = attributes.asInstanceOf[Map[String, AnyVal]]

    new Single(
      casted("graph_id").toInt,
      casted("user_id").toLong,
      casted("item_id").toLong,
      casted("source").toString(),
      casted("action").toString(),
      casted("score").toDouble,
      Time.fromSeconds(casted("create_date").toInt),
      Status(casted("status").toInt),
      CreateType(casted("create_type").toInt),   
      forwardingManager,
      writeSuccesses.toList)
  }
}

class Single(
  graphId: Int,
  userId: Long,
  itemId: Long,
  source: String,
  action: String,
  score: Double,
    createDate: Time,
  status: Status,
  createType: CreateType,
  forwardingManager: ForwardingManager,
  var successes: List[ShardId] = Nil)
  extends JsonJob {

  def toMap = {
    val base = Map(
      "user_id" -> userId,
      "item_id" -> itemId,
      "source" -> source,
      "action" -> action,
      "score" -> score,
      "status" -> status.id,
      "create_type" -> createType.id,
      "create_date" -> createDate.inSeconds,
      "graph_id" -> graphId)

    if (successes.isEmpty) {
      base
    } else {
      base + ("write_successes" -> (successes map { case ShardId(h, tp) => Seq(h, tp) }))
    }
  }

  def apply() = {
    val forward = forwardingManager.findNode(userId, graphId).write

    var currSuccesses: List[ShardId] = Nil
    var currErrs: List[Throwable] = Nil

    val forwardResults = writeToShard(forward, userId, itemId, source,action,score,createDate,status,createType)
    List(forwardResults) foreach {
      _ foreach {
        case Return(id) => currSuccesses = id :: currSuccesses
        case Throw(e) => currErrs = e :: currErrs
      }
    }

    // add successful writes here, since we are only successful if an optimistic lock exception is not raised.
    successes = successes ++ currSuccesses

    currErrs.headOption foreach { e => throw e }
  }

  def writeToShard(shards: NodeSet[Shard], userId: Long, itemId: Long, source: String, action: String,  score: Double,createDate: Time,
    status: Status, createType: CreateType) = {
    shards.skip(successes) all { (shardId, shard) =>

      shard.add(userId, itemId, source, action, score,createDate , status, createType)

      shardId
    }
  }

  override def equals(o: Any) = o match {
    case o: Single => this.toMap == o.toMap
    case _ => false
  }
}


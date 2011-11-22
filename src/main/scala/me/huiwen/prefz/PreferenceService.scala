/*
 * Copyright 2011 IBM, Inc.
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

import com.twitter.gizzard.Stats
import com.twitter.gizzard.util.Future
import com.twitter.gizzard.nameserver.{NameServer, NonExistentShard, InvalidShard}
import com.twitter.gizzard.scheduler.{CopyJobFactory, JsonJob, PrioritizingJobScheduler}
import com.twitter.gizzard.shards.{ShardBlackHoleException, ShardDatabaseTimeoutException,
  ShardOfflineException, ShardTimeoutException}
import com.twitter.gizzard.thrift.conversions.Sequences._
import operations.{ExecuteOperations, SelectOperation}
import com.twitter.logging.Logger
import queries._
import thrift.PrefzException

class PreferenceService(
  forwardingManager: ForwardingManager,
  schedule: PrioritizingJobScheduler,
  future: Future,
  aggregateJobsPageSize: Int) {

  private val log = Logger.get(getClass.getName)
  private val exceptionLog = Logger.get("exception")

  def shutdown() {
    schedule.shutdown()
    future.shutdown()
  }

  def contains(userId: Long, graphId: Int, itemId: Long): Boolean = {
    rethrowExceptionsAsThrift {
      Stats.transaction.name = "contains"
      forwardingManager.find(userId, graphId).get(userId, itemId).map { pref =>
        pref.status== Status.VALID
      }.getOrElse(false)
    }
  }

  def get(userId: Long, graphId: Int, itemId: Long): Preference = {
    rethrowExceptionsAsThrift {
      Stats.transaction.name = "get"
      forwardingManager.find(userId, graphId).get(userId, itemId).getOrElse {
        throw new PrefzException("Record not found: (%d, %d, %d)".format(userId, graphId, itemId))
      }
    }
  }

  def select(query: SelectQuery): ResultWindow[Long] = select(List(query)).head

  def select(queries: Seq[SelectQuery]): Seq[ResultWindow[Long]] = {
    rethrowExceptionsAsThrift {
      queries.parallel(future).map { query =>
        try {
          val queryTree = selectCompiler(query.operations)
          val rv = queryTree.select(query.page)
          Stats.transaction.record(queryTree.toString)
          rv
        } catch {
          case e: ShardBlackHoleException =>
            throw new PrefzException("Shard is blackholed: " + e)
        }
      }
    }
  }

  def selectEdges(queries: Seq[EdgeQuery]): Seq[ResultWindow[Edge]] = {
    rethrowExceptionsAsThrift {
      queries.parallel(future).map { query =>
        val term = query.term
        val shard = forwardingManager.find(term.sourceId, term.graphId, Direction(term.isForward))
        val states = if (term.states.isEmpty) List(State.Normal) else term.states

        if (term.destinationIds.isDefined) {
          val results = shard.intersectEdges(term.sourceId, states, term.destinationIds.get)
          new ResultWindow(results.map { edge => (edge, Cursor(edge.destinationId)) }, query.page.count, query.page.cursor)
        } else {
          shard.selectEdges(term.sourceId, states, query.page.count, query.page.cursor)
        }
      }
    }
  }

  def execute(operations: ExecuteOperations) {
    rethrowExceptionsAsThrift {
      Stats.transaction.name = "execute"
      executeCompiler(operations)
    }
  }

  def count(queries: Seq[Seq[SelectOperation]]): Seq[Int] = {
    rethrowExceptionsAsThrift {
      queries.parallel(future).map { query =>
        val queryTree = selectCompiler(query)
        val rv = queryTree.sizeEstimate
        Stats.transaction.record(queryTree.toString)
        rv
      }
    }
  }

  private def countAndRethrow(e: Throwable) = {
    Stats.incr("exceptions-" + e.getClass.getName.split("\\.").last)
    throw(new PrefzException(e.getMessage))
  }

  private def rethrowExceptionsAsThrift[A](block: => A): A = {
    try {
      block
    } catch {
      case e: NonExistentShard =>
        log.error(e, "NonexistentShard: %s", e)
        throw(new PrefzException(e.getMessage))
      case e: InvalidShard =>
        log.error(e, "NonexistentShard: %s", e)
        throw(new PrefzException(e.getMessage))
      case e: PrefzException =>
        Stats.incr(e.getClass.getName)
        throw(e)
      case e: ShardTimeoutException =>
        countAndRethrow(e)
      case e: ShardDatabaseTimeoutException =>
        countAndRethrow(e)
      case e: ShardOfflineException =>
        countAndRethrow(e)
      case e: Throwable =>
        Stats.incr("exceptions-unknown")
        exceptionLog.error(e, "Unhandled error in EdgesService", e)
        log.error("Unhandled error in EdgesService: " + e.toString)
        throw(new PrefzException(e.toString))
    }
  }
}

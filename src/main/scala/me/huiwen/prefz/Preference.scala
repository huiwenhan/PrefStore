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

import com.twitter.util.Time
import me.huiwen.prefz.jobs.single._
import com.twitter.gizzard.scheduler.{ PrioritizingJobScheduler, JsonJob }

object Preference {
  def apply(userId: Long, itemId: Long, source: String, action: String, updatedAt: Time, score: Double,
    status: Status, createType: CreateType) = new Preference(userId, itemId, source, action, updatedAt.inSeconds, score, status, createType)
}

case class Preference(userId: Long, itemId: Long, source: String, action: String, updatedAtSeconds: Int, score: Double,
  status: Status, createType: CreateType) extends Ordered[Preference] {

  def this(userId: Long, itemId: Long, source: String, action: String, updatedAt: Time, score: Double,
    status: Status, createType: CreateType) =
    this(userId, itemId, source, action, updatedAt.inSeconds, score, status, createType)

  val updatedAt = Time.fromSeconds(updatedAtSeconds)

  def schedule(tableId: Int, forwardingManager: ForwardingManager, scheduler: PrioritizingJobScheduler, priority: Int) = {
    scheduler.put(priority, toJob(tableId, forwardingManager))
  }

  def toJob(tableId: Int, forwardingManager: ForwardingManager) = {

    new Single(
      tableId,
      userId,
      itemId,
      source,
      action,
      score,
      updatedAt,
      status,
      createType,
      forwardingManager)
  }

  def compare(other: Preference) = {
    val out = updatedAt.compare(other.updatedAt)
    if (out == 0) {
      status.compare(other.status)
    } else {
      out
    }
  }

}


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
      userId,
      itemId,
      source,
      action,
      score,
      status,
      createType,
      updatedAt,
      tableId,
      forwardingManager,
      OrderedUuidGenerator)
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


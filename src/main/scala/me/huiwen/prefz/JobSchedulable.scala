package me.huiwen.prefz

import com.twitter.gizzard.scheduler._

trait JobSchedulable {
  def schedule(tableId: Int, forwardingManager: ForwardingManager, scheduler: PrioritizingJobScheduler, priority: Int)
}

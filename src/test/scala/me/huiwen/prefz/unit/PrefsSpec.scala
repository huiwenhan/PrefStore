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
import scala.collection.JavaConversions._
import com.twitter.gizzard.util.Future
import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.shards.ShardInfo
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import org.specs.mock.{ClassMocker, JMocker}
import jobs.single.Single
import conversions.Preference._
import shards.Shard
import thrift.{PrefException, Page, Results}
import Status._


object PrefsSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "Prefs" should {
    val FOLLOWS = 1

    val bob = 1L
    val mary = 2L

    val nestedJob = capturingParam[JsonNestedJob]
    val uuidGenerator = mock[UuidGenerator]
    val forwardingManager = mock[ForwardingManager]
    val shard = mock[Shard]
    val scheduler = mock[PrioritizingJobScheduler]
    val future = mock[Future]
    val flock = new PrefStoreThriftAdapter(new PreferenceService(forwardingManager, scheduler, future,  config.aggregateJobsPageSize), null)




  }
}

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

import com.twitter.util.Time
import org.specs.mock.{ClassMocker, JMocker}
import jobs.single._


object PrefSpec extends ConfiguredSpecification with JMocker with ClassMocker  {
    val now = Time.fromSeconds(124)
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

  val forwardingManager = mock[ForwardingManager]

  "Pref" should {
    "becomes correct job" in {
      val pref = new Preference(user1,item1,source1,action1,now,score1,statusValid,createTypeDefault)
      pref.toJob(graphId1, forwardingManager) mustEqual new Single(graphId1,user1,item1,source1,action1,score1,now,statusValid,createTypeDefault,forwardingManager)
    }
  }
}

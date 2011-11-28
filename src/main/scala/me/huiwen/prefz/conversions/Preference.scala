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
package conversions

import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import me.huiwen.prefz

object Preference {
  class RichPrefzPreference(pref: prefz.Preference) {
    def toThrift = new thrift.Preference(pref.userId, pref.itemId, 
                                   pref.source,pref.action,pref.score,pref.updatedAt.inSeconds, 
                                   thrift.Status.findByValue(pref.status.id),thrift.CreateType.findByValue(pref.createType.id))
  }
  implicit def RichPrefzPreference(pref: prefz.Preference) = new RichPrefzPreference(pref)

  class RichThriftPreference(pref: thrift.Preference) {
    def fromThrift = new prefz.Preference(pref.user_id, pref.item_id,
                                   pref.source,pref.action,Time.fromSeconds(pref.create_date),pref.score,prefz.Status(pref.status.getValue()),prefz.CreateType(pref.create_type.getValue()))
  }
  implicit def RichThriftPreference(pref: thrift.Preference) = new RichThriftPreference(pref)
}

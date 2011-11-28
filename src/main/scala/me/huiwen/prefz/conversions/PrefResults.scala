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

import scala.collection.JavaConversions._
import com.twitter.gizzard.thrift.conversions.Sequences._
import me.huiwen.prefz.conversions.Preference._




object PrefResults {
  class RichResultWindowOfPrefs(resultWindow: ResultWindow[me.huiwen.prefz.Preference]) {
    def toThrift = new thrift.PrefResults(resultWindow.toList.map { _.toThrift }, resultWindow.nextCursor.position,
                                      resultWindow.prevCursor.position)
  }
  implicit def RichResultWindowOfPrefs(resultWindow: ResultWindow[me.huiwen.prefz.Preference]) =
    new RichResultWindowOfPrefs(resultWindow)
}

/*
 * Copyright 2010 Hui Wen Han
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

import com.twitter.gizzard.thrift.conversions.Sequences._
import me.huiwen.prefz


object Status {
  class RichPrefzStatus(status: prefz.Status) {
    def toThrift = status match {
      case prefz.Status.VALID => thrift.Status.VALID
      case prefz.Status.IGNORE =>thrift.Status.IGNORE
    }
  }
  implicit def RichPrefzStatus(status: prefz.Status) = new RichPrefzStatus(status)

  class RichThriftStatus(status: thrift.Status) {
    def fromThrift = status match {
      case thrift.Status.VALID => prefz.Status.VALID
      case thrift.Status.IGNORE => prefz.Status.IGNORE
      
    }
  }
  implicit def RichThriftStatus(status: thrift.Status) = new RichThriftStatus(status)
}

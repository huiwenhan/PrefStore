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


object CreateType {
  class RichPrefzCreateType(createType: prefz.CreateType) {
    def toThrift = createType match {
      case prefz.CreateType.DEFAULT => thrift.CreateType.DEFAULT
      case prefz.CreateType.THUMB =>thrift.CreateType.THUMB
    }
  }
  implicit def RichPrefzCreateType(createType: prefz.CreateType) = new RichPrefzCreateType(createType)

  class RichThriftCreateType(status: thrift.CreateType) {
    def fromThrift = status match {
      case thrift.CreateType.DEFAULT => prefz.CreateType.DEFAULT
      case thrift.CreateType.THUMB => prefz.CreateType.THUMB
      
    }
  }
  implicit def RichThriftCreateType(createType: thrift.CreateType) = new RichThriftCreateType(createType)
}

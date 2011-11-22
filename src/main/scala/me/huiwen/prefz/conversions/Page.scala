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

object Page {
  class RichFlockPage(page: Page) {
    def toThrift = new thrift.Page(page.count, page.cursor.position)
  }
  implicit def richFlockPage(page: Page) = new RichFlockPage(page)

  class RichThriftPage(page: thrift.Page) {
    def fromThrift = new Page(page.count, Cursor(page.cursor))
  }
  implicit def richThriftPage(page: thrift.Page) = new RichThriftPage(page)
}

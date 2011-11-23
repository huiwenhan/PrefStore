/*
 * Copyright 2011 Hui Wen Han.
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

object Cursor {
  def cursorZip(seq: Seq[Long]) = for (i <- seq) yield (i, Cursor(i))

  val End = new Cursor(0)
  val Start = new Cursor(-1)
}

case class Cursor(position: Long) extends Ordered[Cursor] {
  def compare(that: Cursor) = position.compare(that.position)
  def reverse = new Cursor(-position)
  def magnitude = new Cursor(math.abs(position))
}
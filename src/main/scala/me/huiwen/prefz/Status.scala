package me.huiwen.prefz

abstract class Status(val id: Int, val name: String, val ordinal: Int) 
extends Ordered[Status] {
  def compare(s: Status) = ordinal.compare(s.ordinal)
}

object Status {
  def apply(id: Int) = id match {
    case VALID.id => VALID
    case IGNORE.id => IGNORE
  }

  case object VALID extends Status(0, "Normal", 0)
  case object IGNORE extends Status(1, "Negative", 1)

}
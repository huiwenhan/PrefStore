package me.huiwen.prefz

abstract class CreateType(val id: Int, val name: String, val ordinal: Int) 
extends Ordered[CreateType] {
  def compare(s: CreateType) = ordinal.compare(s.ordinal)
}

object CreateType {
  def apply(id: Int) = id match {
    case DEFAULT.id => DEFAULT
    case THUMB.id => THUMB
  }

  case object DEFAULT extends CreateType(0, "DEFAULT", 0)
  case object THUMB extends CreateType(1, "THUMB", 1)

}
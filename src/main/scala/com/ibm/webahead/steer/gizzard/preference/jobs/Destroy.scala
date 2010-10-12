package com.ibm.webahead.steer.gizzard.preference.jobs

import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import com.twitter.gizzard.jobs.UnboundJob


object DestroyParser extends com.twitter.gizzard.jobs.UnboundJobParser[ForwardingManager] {
  def apply(attributes: Map[String, Any]) = {
    new Destroy(
      new Preference(
      attributes("userId").asInstanceOf[AnyVal].toLong,
      attributes("itemId").asInstanceOf[AnyVal].toLong,
      attributes("score").asInstanceOf[BigDecimal].doubleValue,
      attributes("source").asInstanceOf[String],
      attributes("action").asInstanceOf[String],
      attributes("createDate").asInstanceOf[Int],
      attributes("createType").asInstanceOf[Int]) )
      
  }
}

case class Destroy(pref: Preference) extends UnboundJob[ForwardingManager] {
  def toMap = {
    Map(    
    		"userId" -> pref.userId,
    		"itemId" ->pref.itemId,
    		"score" -> pref.score,
    		"source"-> pref.source,
    		"action"-> pref.action, 
    		"createDate"->pref.createDate,
    		"createType" -> pref.createType)
  }

  def apply(forwardingManager: ForwardingManager) = {
    forwardingManager(pref.userId).destroy(pref)
  }
}
package com.ibm.webahead.steer.gizzard.preference.jobs

import com.twitter.gizzard.jobs.UnboundJob
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.Logger

object CreateParser extends com.twitter.gizzard.jobs.UnboundJobParser[ForwardingManager] {
  def apply(attributes: Map[String, Any]) = {
	   val log = Logger.get(getClass.getName)

	  new Create(
      attributes("userId").asInstanceOf[AnyVal].toLong,
      attributes("itemId").asInstanceOf[AnyVal].toLong,
      attributes("score").asInstanceOf[Double],
      attributes("source").asInstanceOf[String],
      attributes("action").asInstanceOf[String],
      attributes("createDate").asInstanceOf[Int],
      attributes("createType").asInstanceOf[Int]
      )
  }
}

case class Create(userId:Long,itemId:Long,score:Double,source:String,action:String,
		createDate: Int, createType: Int) extends UnboundJob[ForwardingManager] {
  def toMap = {
          Map(
    		"userId" -> userId,
    		"itemId" ->itemId,
    		"score" -> score,
    		"source"->source,
    		"action"->action, 
    		"createDate"->createDate,
    		"createType" -> createType)
  }

  def apply(forwardingManager: ForwardingManager) = {
    forwardingManager(userId).create(userId,itemId,score,source,action,createDate,createType)
  }
}
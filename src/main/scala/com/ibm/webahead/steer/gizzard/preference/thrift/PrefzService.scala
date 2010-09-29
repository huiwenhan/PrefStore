package com.ibm.webahead.steer.gizzard.preference

import net.lag.configgy.Config
import com.twitter.gizzard.scheduler.{ PrioritizingJobScheduler }
import jobs.{ Create, Destroy }
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import com.twitter.gizzard.thrift.conversions.Sequences._
import thrift.conversions.Preference._
import java.util.ArrayList;
import com.twitter.results.{Cursor, ResultWindow}
import com.ibm.webahead.steer.gizzard.preference.thrift.conversions.PrefResults._

class PrefzService(forwardingManager: ForwardingManager,
  scheduler: PrioritizingJobScheduler,
  makeId: () => Long)
  extends com.ibm.webahead.steer.gizzard.preference.thrift.PreferenceService.Iface {
  def create(
    userid: Long,
    itemid: Long,
    score: Double,
    source: String,
    action: String,
    createdate: Int,
    createtype: Int) = {
    scheduler(3)(new Create(
      userid,
      itemid,
      score,
      source,
      action,
      createdate,
      createtype))
    userid
  }

  def destroy(pref: thrift.Preference) {
    scheduler(3)(new Destroy(pref.fromThrift))
  }

  def read(userId: Long) = {
    //forwardingManager(id).read(id).get.toThrift

    val pref = forwardingManager(userId).read(userId)
    if (pref != None) {
       var javaList = pref.map{ _.toThrift }.toJavaList
      //var javaList = new ArrayList[thrift.Preference]
      //javaList.add(pref.toPrefResults)
      javaList
     
    } else {
      null
    }
  }

  def selectPreferencesBySourcAndAction(source: String, action: String): ArrayList[thrift.Preference] =
  {
      selectPreferencesBySourcAndAction(source, action)
      var javaList = new ArrayList[thrift.Preference]
      //javaList.add(pref.toPrefResults)
      javaList
  }

}
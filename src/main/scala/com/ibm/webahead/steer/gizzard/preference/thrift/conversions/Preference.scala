package com.ibm.webahead.steer.gizzard.preference.thrift.conversions

import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._

object Preference {
  class RichShardingPref(pref: com.ibm.webahead.steer.gizzard.preference.Preference) {
    def toThrift = new thrift.Preference(
    		
    		pref.userId,
    		pref.itemId,
    		pref.score,
    		pref.source,
    		pref.action,
    		pref.createDate,
    		pref.createType
    		)
  }
  implicit def shardingPrefToRichShardingPref(pref: preference.Preference) = new RichShardingPref(pref)

  class RichThriftPref(pref: com.ibm.webahead.steer.gizzard.preference.thrift.Preference) {
    def fromThrift = new preference.Preference(
    		
    		pref.userid,
    		pref.itemid,
    		pref.score,
    		pref.source,
    		pref.action,
    		pref.createdate,
    		pref.createtype)
  }
  implicit def thriftPrefToRichThriftPref(pref: thrift.Preference) = new RichThriftPref(pref)
}

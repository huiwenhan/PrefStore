package com.ibm.webahead.steer.gizzard.preference

import com.twitter.gizzard.shards
import com.twitter.xrayspecs.Time
import com.twitter.results.{Cursor, ResultWindow}
import scala.collection.mutable


trait Shard extends com.twitter.gizzard.shards.Shard {
  def create(userId:Long,
		  itemId:Long,score:Double,
		  source:String,action:String,
		  create: Int, 
		  createType: Int)
  def destroy(pref: Preference)
  def read(id: Long):mutable.ArrayBuffer[preference.Preference]
  def selectAll(cursor: Cursor, count: Int):(Seq[Preference], Option[Cursor])
  def write(prefs: Seq[Preference])
  def selectPreferencesBySourcAndAction(source: String, action: String):Seq[Preference]
}
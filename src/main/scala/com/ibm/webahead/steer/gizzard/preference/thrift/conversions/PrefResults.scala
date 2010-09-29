package com.ibm.webahead.steer.gizzard.preference.thrift.conversions
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.results.ResultWindow
import conversions.Preference._

object PrefResults {

 class RichResultWindowOfPrefs(resultWindow: ResultWindow[preference.Preference]) {
    def toPrefResults = new thrift.PrefResults(resultWindow.map { _.toThrift }.toJavaList,
               resultWindow.nextCursor.position, resultWindow.prevCursor.position)
  }
  implicit def richResultWindowOfPrefs(resultWindow: ResultWindow[preference.Preference]) =
    new RichResultWindowOfPrefs(resultWindow)
}
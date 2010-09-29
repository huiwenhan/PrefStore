package com.ibm.webahead.steer.gizzard.preference

import com.twitter.gizzard.proxy.ExceptionHandlingProxy
import net.lag.logging.Logger


object ExceptionWrappingProxy extends ExceptionHandlingProxy({e =>
  val log = Logger.get

  log.error(e, "Error in Prefz: " + e)
  throw new thrift.PreferencezException(e.toString)
})


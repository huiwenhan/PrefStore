package com.ibm.webahead.steer.gizzard.preference

import com.twitter.gizzard.jobs.CopyFactory
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.proxy.LoggingProxy
import com.twitter.gizzard.scheduler.{PrioritizingJobScheduler}
import com.twitter.gizzard.thrift.{TSelectorServer, JobManager, JobManagerService, ShardManager,
  ShardManagerService}
import com.twitter.ostrich.{W3CStats, Stats}
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.{Configgy, RuntimeEnvironment, ConfigMap}
import net.lag.logging.Logger
import org.apache.thrift.server.{TServer, TThreadPoolServer}
import org.apache.thrift.transport.{TServerSocket, TTransportFactory}



object Main {
  var state: Preferencez.State = null
  var prefServer: TSelectorServer = null
  var jobServer: TSelectorServer = null
  var shardServer: TSelectorServer = null

  var config: ConfigMap = null
  val runtime = new RuntimeEnvironment(getClass)

  def main(args: Array[String]) {
	  
	try {
		runtime.load(args)
		 config = Configgy.config
	}catch{
		
		case e =>
        println("Exception in load args!",e)
        shutdown()
	}
	
    try {
      
     
      val w3c = new W3CStats(Logger.get("w3c"), config.getList("prefz.w3c").toArray)
      state = Preferencez(config, w3c)
      state.start()
      startThrift(w3c)
      println("Running.")
    } catch {
      case e =>
      	throw e
        println("Exception in initialization!",e)
        shutdown()
    }
  }

  def startThrift(w3c: W3CStats) {
    val timeout = config("prefz.timeout_msec").toInt.milliseconds
    val idleTimeout = config("prefz.idle_timeout_sec").toInt.seconds
    val executor = TSelectorServer.makeThreadPoolExecutor(config.configMap("prefz"))
    
    val processor = new preference.thrift.PreferenceService.Processor(LoggingProxy[preference.thrift.PreferenceService.Iface](Stats, w3c, "prefz", state.prefzService))
      /*
    val processor = new com.ibm.webahead.steer.gizzard.preference.thrift.Preferencez.Processor
    	(LoggingProxy[com.ibm.webahead.steer.gizzard.preference.thrift.Preferencez.Iface]
    	(Stats, w3c, "prefz", state.prefzService))
  
    val processor = new preference.thrift.Preferencez.Processor
    	(LoggingProxy[preference.thrift.Preferencez.Iface](Stats, w3c, "Prefz", state.prefzService))
    
    */
    prefServer = TSelectorServer("prefz", config("prefz.server_port").toInt, processor, executor, timeout, idleTimeout)

    val jobManagerService = new JobManagerService(state.prioritizingScheduler)
    val jobProcessor = new JobManager.Processor(LoggingProxy[JobManager.Iface](Stats, w3c, "prefzJobs", jobManagerService))
    jobServer = TSelectorServer("prefz-jobs", config("prefz.job_server_port").toInt, jobProcessor, executor, timeout, idleTimeout)

    val shardManagerService = new ShardManagerService(state.nameServer, state.copyFactory, state.prioritizingScheduler(2))
    val shardProcessor = new ShardManager.Processor(ExceptionWrappingProxy(LoggingProxy[ShardManager.Iface](Stats, w3c, "prefzShards", shardManagerService)))
    shardServer = TSelectorServer("prefz-shards", config("prefz.shard_server_port").toInt, shardProcessor, executor, timeout, idleTimeout)

    prefServer.serve()
    jobServer.serve()
    shardServer.serve()
  }

  def shutdown() {
    try {
      prefServer.stop()
      jobServer.stop()
      state.shutdown()
    } finally {
      println("Exiting!")
      System.exit(0)
    }
  }
}

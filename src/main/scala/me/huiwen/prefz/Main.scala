package me.huiwen.prefz
import com.twitter.util.Eval
import com.twitter.logging.Logger
import com.twitter.ostrich.admin.{Service, ServiceTracker, RuntimeEnvironment, AdminHttpService}
import java.io.File

import me.huiwen.prefz.config.{PrefStore => PrefStoreConfig}
object  Main {

    val log = Logger.get

  var adminServer: Option[AdminHttpService] = None

  def main(args: Array[String]) {
    try {
      log.info("Starting PrefStore.")

      val eval = new Eval
      val config = eval[PrefStoreConfig](args.map(new File(_)): _*)
      val runtime = new RuntimeEnvironment(this)

      Logger.configure(config.loggers)
      adminServer = config.adminConfig()(runtime)

      val service = new PrefStore(config)

      ServiceTracker.register(service)
      service.start()

    } catch {
      case e => {
        log.fatal(e, "Exception in initialization: ", e.getMessage)
        log.fatal(e.getStackTrace.toString)
        System.exit(1)
      }
    }
  }
}
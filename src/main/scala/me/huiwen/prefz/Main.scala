/*
 * Copyright 2011 Hui Wen Han, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
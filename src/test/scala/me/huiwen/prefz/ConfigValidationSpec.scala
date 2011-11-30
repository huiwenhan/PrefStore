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

import org.specs.Specification
import com.twitter.util.Eval
import java.io.File
import me.huiwen.prefz


object ConfigValidationSpec extends Specification {
  "Configuration Validation" should {
    "production.scala" >> {
      var eval = new Eval()
      val config = Eval[prefz.config.PrefStore](new File("config/production.scala"))
      config mustNot beNull
    }
    "development.scala" >> {
      val config = Eval[prefz.config.PrefStore](new File("config/development.scala"))
      config mustNot beNull
    }

    "test.scala" >> {
      val config = Eval[prefz.config.PrefStore](new File("config/test.scala"))
      config mustNot beNull
    }
  }
}

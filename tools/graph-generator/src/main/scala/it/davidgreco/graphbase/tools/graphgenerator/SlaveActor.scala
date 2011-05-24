/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package it.davidgreco.graphbase.tools.graphgenerator

import com.tinkerpop.blueprints.pgm.IndexableGraph
import akka.actor.{PoisonPill, Actor}
import akka.actor.Actor._
import it.davidgreco.graphbase.blueprints.HBaseGraph
class SlaveActor extends Actor {

  var graph: IndexableGraph = _

  protected def receive = {
    case ConnectSlaves(quorum, port, name) => {
      graph = new HBaseGraph(quorum, port, name)
      self.reply("OK")
    }
    case GenerateRange(x, y) => {
      for (i <- x to y) {
        val v = graph.addVertex(null)
        v.setProperty("id", i)
        if (i % 100 == 0)
          println(this + " " + i)
      }
      println("done.")
    }
  }
}
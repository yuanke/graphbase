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

import akka.actor.Actor._
import akka.actor.{ActorRef, PoisonPill, Actor}
class MasterActor extends Actor {

  var slaves: IndexedSeq[ActorRef] = _

  protected def receive = {

    case CreateSlaves(numSlaves) => {
      slaves = for {i <- 1 to numSlaves; slave = actorOf(new SlaveActor).start()} yield slave
    }

    case msg: ConnectSlaves => {
      for (slave <- slaves) {
        slave !! msg
      }
    }

    case GenerateGraph(numVerticesPerSlave) => {
      for {
        i <- 1 to slaves.size
        range = ((i - 1) * numVerticesPerSlave + 1, (i - 1) * numVerticesPerSlave + numVerticesPerSlave)
      } {
        slaves.apply(i - 1) ! GenerateRange(range._1, range._2)
      }
    }

    case ShutdownSlaves => {
      for (slave <- slaves) {
        slave ! PoisonPill
      }
    }
  }
}

object Main extends App {

  val master = actorOf(new MasterActor).start()

  master ! CreateSlaves(5)

  master ! ConnectSlaves("cloudera-vm", "2181", "simple")

  master ! GenerateGraph(1000)

  master ! ShutdownSlaves

  master ! PoisonPill

}
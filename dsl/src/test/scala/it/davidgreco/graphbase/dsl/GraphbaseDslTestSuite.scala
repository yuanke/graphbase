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
package it.davidgreco.graphbase.dsl

import scala.collection.JavaConversions._
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{BeforeAndAfterEach, Spec}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.Bytes
import it.davidgreco.graphbase.blueprints.{EmbeddedHbase, HBaseGraph}

class GraphbaseDslTestSuite extends Spec with ShouldMatchers with BeforeAndAfterEach with EmbeddedHbase {

  val port = "21818"

  describe("A DSL for graph") {

    it("should create and remove vertices") {
      val conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", "localhost")
      conf.set("hbase.zookeeper.property.clientPort", port)
      val admin = new HBaseAdmin(conf)
      val g = new HBaseGraph(admin, "simple")

      val G = graph(g)

      val idx1 = G +=| ("simple1", "ID" :: Nil)
      val idx2 = G +=|| ("simple2", "ID" :: Nil)

      for (i <- 1 to 10) {
        +G <= ("ID", i)
      }

      for (i <- 1 to 10) {
        val v = idx1.index.get("ID", i)
        println(v.toBuffer.apply(0).getProperty("ID"))
      }

      val v1 = +G <= ("CICCIO", "PIPPO")
      val v2 = +G

      val e1 = G <= (v1, "likes", v2) <= ("CICCIO", "PAPPO")

      G -=| "simple1" -=| "simple2"

    }

  }

  def toString(id: AnyRef): String = Bytes.toString(id.asInstanceOf[Array[Byte]]);

}
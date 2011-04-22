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

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{BeforeAndAfterEach, Spec}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.Bytes
import it.davidgreco.graphbase.blueprints.EmbeddedHbase

class GraphbaseDslTestSuite extends Spec with ShouldMatchers with BeforeAndAfterEach with EmbeddedHbase {

  val port = "21818"

  describe("A graph") {

    it("should create and retrieve vertexes") {
      var conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", "localhost")
      conf.set("hbase.zookeeper.property.clientPort", port)
      val admin = new HBaseAdmin(conf)

      val G = new graph(admin, "simple")

      val v1 = +G
      val v3 = G ?\/ ~v1

      assert(toString(~v1) == toString(~v3.get))
    }

    it("should add and retrieve vertex properties") {
      var conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", "localhost")
      conf.set("hbase.zookeeper.property.clientPort", port)
      val admin = new HBaseAdmin(conf)

      val G = new graph(admin, "simple")

      val v1 = +G

      v1 <= ("A_STRING", "DAVID")
      v1 <= ("A_LONG", 1234567L)
      v1 <= ("AN_INT", 123456)
      v1 <= ("A_SHORT", 1234)
      v1 <= ("A_FLOAT", 3.1415926535F)
      v1 <= ("A_DOUBLE", 3.1415926535D)
      v1 <= ("A_BOOLEAN", true)

      assert((v1 >= "A_STRING") == Some("DAVID"))
      assert((v1 >= "A_LONG") == Some(1234567L))
      assert((v1 >= "AN_INT") == Some(123456))
      assert((v1 >= "A_SHORT") == Some(1234))
      assert((v1 >= "A_FLOAT") == Some(3.1415926535F))
      assert((v1 >= "A_DOUBLE") == Some(3.1415926535D))
      assert((v1 >= "A_BOOLEAN") == Some(true))

    }

    it("should remove vertexes") {
      var conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", "localhost")
      conf.set("hbase.zookeeper.property.clientPort", port)
      val admin = new HBaseAdmin(conf)
      val G = new graph(admin, "simple")

      val v1 = +G

      G - v1

      assert(G ?\/ ~v1 == None)
    }

    it("should remove vertex properties") {
      var conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", "localhost")
      conf.set("hbase.zookeeper.property.clientPort", port)
      val admin = new HBaseAdmin(conf)
      val G = new graph(admin, "simple")

      val v1 = +G

      v1 <= ("A_STRING", "DAVID")
      assert((v1 -= "A_STRING") == Some("DAVID"))
      assert((v1 >= "A_STRING") == None)
    }

    it("should complain on non supported property type") {
      var conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", "localhost")
      conf.set("hbase.zookeeper.property.clientPort", port)
      val admin = new HBaseAdmin(conf)
      val G = new graph(admin, "simple")

      val v1 = +G

      intercept[RuntimeException] {
        v1 <= ("NOT_SUPPORTED", List("A"))
      }
    }

    it("should create and retrieve edges") {
      var conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", "localhost")
      conf.set("hbase.zookeeper.property.clientPort", port)
      val admin = new HBaseAdmin(conf)
      val G = new graph(admin, "simple")

      val v1 = +G
      val v2 = +G

      val e1 = G + (v1, "e1", v2)

      val e1n = G ?-- ~e1

      assert(toString(~(e1n.get =>\/)) == toString(~v1))
      assert(toString(~(e1n.get <=\/)) == toString(~v2))
      assert((e1n.get L) == "e1")
    }

    it("should create and retrieve edge properties") {
      var conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", "localhost")
      conf.set("hbase.zookeeper.property.clientPort", port)
      val admin = new HBaseAdmin(conf)
      val G = new graph(admin, "simple")

      val v1 = +G
      val v2 = +G

      val e1 = G + (v1, "e1", v2)

      e1 <= ("A_STRING", "DAVID")
      e1 <= ("A_LONG", 1234567L)
      e1 <= ("AN_INT", 123456)
      e1 <= ("A_SHORT", 1234)
      e1 <= ("A_FLOAT", 3.1415926535F)
      e1 <= ("A_DOUBLE", 3.1415926535D)
      e1 <= ("A_BOOLEAN", true)

      assert((e1 >= "A_STRING") == Option("DAVID"))
      assert((e1 >= "A_LONG") == Option(1234567L))
      assert((e1 >= "AN_INT") == Option(123456))
      assert((e1 >= "A_SHORT") == Option(1234))
      assert((e1 >= "A_FLOAT") == Option(3.1415926535F))
      assert((e1 >= "A_DOUBLE") == Option(3.1415926535D))
      assert((e1 >= "A_BOOLEAN") == Option(true))
      //assert(e2.getPropertyKeys.toSet == Set("A_STRING", "A_LONG", "AN_INT", "A_SHORT", "A_FLOAT", "A_DOUBLE", "A_BOOLEAN"))
    }
    /*
    it("should remove edge properties") {
      var conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", "localhost")
      conf.set("hbase.zookeeper.property.clientPort", port)
      val admin = new HBaseAdmin(conf)
      val graph: Graph = new HBaseGraph(admin, "simple")
      val v1 = graph.addVertex(null)
      val v2 = graph.addVertex(null)
      val e1 = graph.addEdge(null, v1, v2, "LABEL")

      e1.setProperty("A_STRING", "DAVID")
      assert(e1.removeProperty("A_STRING") == "DAVID")
      assert(e1.getProperty("A_STRING") == null)
    }

    it("should remove edges") {
      var conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", "localhost")
      conf.set("hbase.zookeeper.property.clientPort", port)
      val admin = new HBaseAdmin(conf)
      val graph: Graph = new HBaseGraph(admin, "simple")

      val v1 = graph.addVertex(null)
      val v2 = graph.addVertex(null)
      val v3 = graph.addVertex(null)
      val e1 = graph.addEdge(null, v1, v2, "e1")
      val e2 = graph.addEdge(null, v1, v3, "e2")

      assert(v2.getInEdges.size == 1)
      assert(v1.getOutEdges.size == 2)
      assert(v3.getInEdges.size == 1)

      e1.setProperty("A_STRING", "DAVID")
      e1.setProperty("A_LONG", 1234567L)
      e1.setProperty("AN_INT", 123456)
      e1.setProperty("A_SHORT", 1234)
      e1.setProperty("A_FLOAT", 3.1415926535F)
      e1.setProperty("A_DOUBLE", 3.1415926535D)
      e1.setProperty("A_BOOLEAN", true)

      e2.setProperty("A_STRING", "DAVID")
      e2.setProperty("A_LONG", 1234567L)
      e2.setProperty("AN_INT", 123456)
      e2.setProperty("A_SHORT", 1234)
      e2.setProperty("A_FLOAT", 3.1415926535F)
      e2.setProperty("A_DOUBLE", 3.1415926535D)
      e2.setProperty("A_BOOLEAN", true)

      graph.removeEdge(e1);
      assert(v2.getInEdges.size == 0)
      assert(v1.getOutEdges.size == 1)
      assert(v3.getInEdges.size == 1)

      graph.removeEdge(e2);
      assert(v2.getInEdges.size == 0)
      assert(v1.getOutEdges.size == 0)
      assert(v3.getInEdges.size == 0)

      val e1n = graph.getEdge(e1.getId)
      val e2n = graph.getEdge(e2.getId)

      assert(e1n == null)
      assert(e2n == null)

      assert(e1.getProperty("A_STRING") == null)
      assert(e1.getProperty("A_LONG") == null)
      assert(e1.getProperty("AN_INT") == null)
      assert(e1.getProperty("A_SHORT") == null)
      assert(e1.getProperty("A_FLOAT") == null)
      assert(e1.getProperty("A_DOUBLE") == null)
      assert(e1.getProperty("A_BOOLEAN") == null)

      assert(e2.getProperty("A_STRING") == null)
      assert(e2.getProperty("A_LONG") == null)
      assert(e2.getProperty("AN_INT") == null)
      assert(e2.getProperty("A_SHORT") == null)
      assert(e2.getProperty("A_FLOAT") == null)
      assert(e2.getProperty("A_DOUBLE") == null)
      assert(e2.getProperty("A_BOOLEAN") == null)
    }*/
  }

  def toString(id: AnyRef): String = Bytes.toString(id.asInstanceOf[Array[Byte]]);

}
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
package it.davidgreco.graphbase.blueprints

import scala.collection.JavaConversions._
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{BeforeAndAfterEach, Spec}
import org.apache.hadoop.hbase.util.Bytes
import com.tinkerpop.blueprints.pgm.{Edge, Vertex, IndexableGraph}

class GraphbaseIndexTestSuite extends Spec with ShouldMatchers with BeforeAndAfterEach with EmbeddedHBase {

  val port = "21818"

  describe("A graph") {

    it("should create and remove automatic indexes") {
      val graph: IndexableGraph = new HBaseGraph("localhost", port, "simple")

      val i1 = graph.createAutomaticIndex("idx1", classOf[Vertex], Set("FirstName", "FamilyName"))
      val indexes1 = graph.getIndices
      val i1n = graph.getIndex("idx1", classOf[Vertex])

      assert(indexes1.size == 1)

      graph.dropIndex("idx1")

      val indexes2 = graph.getIndices
      assert(indexes2.size == 0)
    }

    it("should index vertex properties") {
      val graph: IndexableGraph = new HBaseGraph("localhost", port, "simple")

      val i1 = graph.createAutomaticIndex("idx1", classOf[Vertex], Set("FirstName", "FamilyName"))
      val i1n = graph.getIndex("idx1", classOf[Vertex])

      val v1 = graph.addVertex(null)
      v1.setProperty("FirstName", "David")
      v1.setProperty("FamilyName", "Greco")

      val v1n1 = i1n.get("FirstName", "David")
      val v1n2 = i1n.get("FamilyName", "Greco")

      assert((for (v <- v1n1.iterator()) yield v).length == 1)
      assert((for (v <- v1n2.iterator()) yield v).length == 1)
      assert(toString(v1n1.next().getId) == toString(v1n2.next().getId))
    }

    it("should index edge properties") {
      val graph: IndexableGraph = new HBaseGraph("localhost", port, "simple")

      val i1 = graph.createAutomaticIndex("idx2", classOf[Edge], Set("Prop1", "Prop2"))
      val i1n = graph.getIndex("idx2", classOf[Edge])

      val v1 = graph.addVertex(null)
      val v2 = graph.addVertex(null)
      val e1 = graph.addEdge(null, v1, v2, "")
      e1.setProperty("Prop1", "Val1")
      e1.setProperty("Prop2", "Val2")

      val e1n1 = i1n.get("Prop1", "Val1")
      val e1n2 = i1n.get("Prop2", "Val2")
      assert((for (v <- e1n1.iterator()) yield v).length == 1)
      assert((for (v <- e1n2.iterator()) yield v).length == 1)
      assert(toString(e1n1.next().getId) == toString(e1n2.next().getId))
    }

    it("shouldn't allow an index defined for edges to index vertices") {
      val graph: IndexableGraph = new HBaseGraph("localhost", port, "simple")

      val iv = graph.createAutomaticIndex("idx3", classOf[Edge], Set("Prop1"))

      val v1 = graph.addVertex(null)
      v1.setProperty("Prop1", "Val1")
      val v1a = iv.get("Prop1", "Val1")
      assert((for (v <- v1a.iterator()) yield v).length == 0)
    }

    it("shouldn't allow an index defined for vertices to index edges") {
      val graph: IndexableGraph = new HBaseGraph("localhost", port, "simple")

      val ie = graph.createAutomaticIndex("idx4", classOf[Vertex], Set("Prop1"))

      val v1 = graph.addVertex(null)
      val v2 = graph.addVertex(null)
      val e1 = graph.addEdge(null, v1, v2, "")
      e1.setProperty("Prop1", "Val1")
      val e1a = ie.get("Prop1", "Val1")
      assert((for (v <- e1a.iterator()) yield v).length == 0)
    }

  }

  def toString(id: AnyRef): String = Bytes.toString(id.asInstanceOf[Array[Byte]]);

}
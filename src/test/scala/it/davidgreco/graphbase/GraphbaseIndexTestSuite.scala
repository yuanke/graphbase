package it.davidgreco.graphbase

import scala.collection.JavaConversions._
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{BeforeAndAfterEach, Spec}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.Bytes
import com.tinkerpop.blueprints.pgm.{Edge, Vertex, IndexableGraph}

class GraphbaseIndexTestSuite extends Spec with ShouldMatchers with BeforeAndAfterEach with EmbeddedHbase {

  val port = "21818"

  describe("A graph") {

    it("should create and use manual indexes") {
      var conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", "localhost")
      conf.set("hbase.zookeeper.property.clientPort", port)
      val admin = new HBaseAdmin(conf)
      val graph: IndexableGraph = new HBaseGraph(admin, "simple")

      val v1 = graph.addVertex(null)
      val v2 = graph.addVertex(null)
      val v3 = graph.addVertex(null)
      val v4 = graph.addVertex(null)
      val e1 = graph.addEdge(null, v1, v2, "e1")
      val e2 = graph.addEdge(null, v1, v3, "e2")

      val index  = graph.createManualIndex("test-idx_v", classOf[Vertex])

      index.put("Name", "David", v1)
      index.put("Name", "David", v4)
      index.put("Name", "Paolo", v2)
      index.put("Name", "Marco", v3)

      val rindex = graph.getIndex("test-idx_v", classOf[Vertex])

      val v1n = rindex.get("Name", "David").toIndexedSeq
      assert(v1n.length == 2)
      assert(toString(v1n.apply(0).getId) == toString(v1.getId))
      assert(toString(v1n.apply(1).getId) == toString(v4.getId))
      rindex.remove("Name", "David", v4)

      assert(rindex.get("Name", "David").toIndexedSeq.length == 1)

      val v2n = rindex.get("Name", "Paolo").toIndexedSeq
      assert(v2n.length == 1)
      assert(toString(v2n.apply(0).getId) == toString(v2.getId))

      val v3n = rindex.get("Name", "Marco").toIndexedSeq
      assert(v3n.length == 1)
      assert(toString(v3n.apply(0).getId) == toString(v3.getId))

      val eindex = graph.createManualIndex("test-idx_e", classOf[Edge])

      eindex.put("Label", "E1", e1)
      eindex.put("Label", "E2", e2)

      val e1n = eindex.get("Label", "E1").toIndexedSeq
      assert(e1n.length == 1)
      assert(toString(e1n.apply(0).getId) == toString(e1.getId))

      val e2n = eindex.get("Label", "E2").toIndexedSeq
      assert(e2n.length == 1)
      assert(toString(e2n.apply(0).getId) == toString(e2.getId))

    }

  }

  def toString(id: AnyRef): String = Bytes.toString(id.asInstanceOf[Array[Byte]]);

}
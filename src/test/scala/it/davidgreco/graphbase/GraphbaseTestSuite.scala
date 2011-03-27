package it.davidgreco.graphbase

import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{BeforeAndAfterEach, Spec}
import org.junit.runner.RunWith
import org.apache.hadoop.hbase.HBaseConfiguration
import com.tinkerpop.blueprints.pgm.Graph
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class GraphbaseTestSuite extends Spec with ShouldMatchers with BeforeAndAfterEach with EmbeddedHbase {

  val port = "21818"

  describe("A graph") {

    it("should create and retrieve vertexes") {
      var conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", "localhost")
      conf.set("hbase.zookeeper.property.clientPort", port)
      val admin = new HBaseAdmin(conf)
      val graph: Graph = new HBaseGraph(admin, "simple")

      val v1 = graph.addVertex(null);

      val v3 = graph.getVertex(v1.getId);

      assert(toString(v1.getId) == toString(v3.getId))
    }

    it("should add and retrieve vertex properties") {
      var conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", "localhost")
      conf.set("hbase.zookeeper.property.clientPort", port)
      val admin = new HBaseAdmin(conf)
      val graph: Graph = new HBaseGraph(admin, "simple")
      val v1 = graph.addVertex(null)

      v1.setProperty("A_STRING", "DAVID")
      v1.setProperty("A_LONG", 1234567L)
      v1.setProperty("AN_INT", 123456)
      v1.setProperty("A_SHORT", 1234)
      v1.setProperty("A_FLOAT", 3.1415926535F)
      v1.setProperty("A_DOUBLE", 3.1415926535D)
      v1.setProperty("A_BOOLEAN", true)

      assert(v1.getProperty("A_STRING") == "DAVID")
      assert(v1.getProperty("A_LONG") == 1234567L)
      assert(v1.getProperty("AN_INT") == 123456)
      assert(v1.getProperty("A_SHORT") == 1234)
      assert(v1.getProperty("A_FLOAT") == 3.1415926535F)
      assert(v1.getProperty("A_DOUBLE") == 3.1415926535D)
      assert(v1.getProperty("A_BOOLEAN") == true)

    }

    it("should remove vertexes") {
      var conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", "localhost")
      conf.set("hbase.zookeeper.property.clientPort", port)
      val admin = new HBaseAdmin(conf)
      val graph: Graph = new HBaseGraph(admin, "simple")
      val v1 = graph.addVertex(null)

      graph.removeVertex(v1)

      assert(graph.getVertex(v1.getId) == null)
    }

    it("should remove vertex properties") {
      var conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", "localhost")
      conf.set("hbase.zookeeper.property.clientPort", port)
      val admin = new HBaseAdmin(conf)
      val graph: Graph = new HBaseGraph(admin, "simple")
      val v1 = graph.addVertex(null)

      v1.setProperty("A_STRING", "DAVID")
      assert(v1.removeProperty("A_STRING") == "DAVID")
      assert(v1.getProperty("A_STRING") == null)
    }

    it("should complain on non supported property type") {
      var conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", "localhost")
      conf.set("hbase.zookeeper.property.clientPort", port)
      val admin = new HBaseAdmin(conf)
      val graph: Graph = new HBaseGraph(admin, "simple")

      val v1 = graph.addVertex(null);

      intercept[RuntimeException] {
        v1.setProperty("NOT_SUPPORTED", List("A"))
      }
    }

    it("should create and retrieve edges") {
      var conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", "localhost")
      conf.set("hbase.zookeeper.property.clientPort", port)
      val admin = new HBaseAdmin(conf)
      val graph: Graph = new HBaseGraph(admin, "simple")

      val v1 = graph.addVertex(null)
      val v2 = graph.addVertex(null)
      val v3 = graph.addVertex(null)
      val v4 = graph.addVertex(null)
      val v5 = graph.addVertex(null)

      val e1 = graph.addEdge(null, v1, v2, "e1")
      val e2 = graph.addEdge(null, v1, v3, "e2")
      val e3 = graph.addEdge(null, v2, v3, "e3")
      val e4 = graph.addEdge(null, v2, v4, "e4")
      val e5 = graph.addEdge(null, v3, v2, "e5")
      val e6 = graph.addEdge(null, v3, v4, "e6")
      val e7 = graph.addEdge(null, v3, v5, "e7")
      val e8 = graph.addEdge(null, v4, v5, "e8")
      val e9 = graph.addEdge(null, v5, v4, "e9")
      val e10 = graph.addEdge(null, v5, v1, "e10")

      val e1n = graph.getEdge(e1.getId)
      assert(toString(e1n.getOutVertex.getId) == toString(v1.getId))
      assert(toString(e1n.getInVertex.getId) == toString(v2.getId))
      assert(e1n.getLabel == "e1")

      val e2n = graph.getEdge(e2.getId)
      assert(toString(e2n.getOutVertex.getId) == toString(v1.getId))
      assert(toString(e2n.getInVertex.getId) == toString(v3.getId))
      assert(e2n.getLabel == "e2")

      val e3n = graph.getEdge(e3.getId)
      assert(toString(e3n.getOutVertex.getId) == toString(v2.getId))
      assert(toString(e3n.getInVertex.getId) == toString(v3.getId))
      assert(e3n.getLabel == "e3")

      val e4n = graph.getEdge(e4.getId)
      assert(toString(e4n.getOutVertex.getId) == toString(v2.getId))
      assert(toString(e4n.getInVertex.getId) == toString(v4.getId))
      assert(e4n.getLabel == "e4")

      val e5n = graph.getEdge(e5.getId)
      assert(toString(e5n.getOutVertex.getId) == toString(v3.getId))
      assert(toString(e5n.getInVertex.getId) == toString(v2.getId))
      assert(e5n.getLabel == "e5")

      val e6n = graph.getEdge(e6.getId)
      assert(toString(e6n.getOutVertex.getId) == toString(v3.getId))
      assert(toString(e6n.getInVertex.getId) == toString(v4.getId))
      assert(e6n.getLabel == "e6")

      val e7n = graph.getEdge(e7.getId)
      assert(toString(e7n.getOutVertex.getId) == toString(v3.getId))
      assert(toString(e7n.getInVertex.getId) == toString(v5.getId))
      assert(e7n.getLabel == "e7")

      val e8n = graph.getEdge(e8.getId)
      assert(toString(e8n.getOutVertex.getId) == toString(v4.getId))
      assert(toString(e8n.getInVertex.getId) == toString(v5.getId))
      assert(e8n.getLabel == "e8")

      val e9n = graph.getEdge(e9.getId)
      assert(toString(e9n.getOutVertex.getId) == toString(v5.getId))
      assert(toString(e9n.getInVertex.getId) == toString(v4.getId))
      assert(e9n.getLabel == "e9")

      val e10n = graph.getEdge(e10.getId)
      assert(toString(e10n.getOutVertex.getId) == toString(v5.getId))
      assert(toString(e10n.getInVertex.getId) == toString(v1.getId))
      assert(e10n.getLabel == "e10")
    }

    it("should create and retrieve edge properties") {
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

      e1.setProperty("A_STRING", "DAVID")
      e1.setProperty("A_LONG", 1234567L)
      e1.setProperty("AN_INT", 123456)
      e1.setProperty("A_SHORT", 1234)
      e1.setProperty("A_FLOAT", 3.1415926535F)
      e1.setProperty("A_DOUBLE", 3.1415926535D)
      e1.setProperty("A_BOOLEAN", true)

      assert(e1.getProperty("A_STRING") == "DAVID")
      assert(e1.getProperty("A_LONG") == 1234567L)
      assert(e1.getProperty("AN_INT") == 123456)
      assert(e1.getProperty("A_SHORT") == 1234)
      assert(e1.getProperty("A_FLOAT") == 3.1415926535F)
      assert(e1.getProperty("A_DOUBLE") == 3.1415926535D)
      assert(e1.getProperty("A_BOOLEAN") == true)

      e2.setProperty("A_STRING", "DAVID")
      e2.setProperty("A_LONG", 1234567L)
      e2.setProperty("AN_INT", 123456)
      e2.setProperty("A_SHORT", 1234)
      e2.setProperty("A_FLOAT", 3.1415926535F)
      e2.setProperty("A_DOUBLE", 3.1415926535D)
      e2.setProperty("A_BOOLEAN", true)

      assert(e2.getProperty("A_STRING") == "DAVID")
      assert(e2.getProperty("A_LONG") == 1234567L)
      assert(e2.getProperty("AN_INT") == 123456)
      assert(e2.getProperty("A_SHORT") == 1234)
      assert(e2.getProperty("A_FLOAT") == 3.1415926535F)
      assert(e2.getProperty("A_DOUBLE") == 3.1415926535D)
      assert(e2.getProperty("A_BOOLEAN") == true)

      assert(e2.getPropertyKeys.toSet == Set("A_STRING", "A_LONG", "AN_INT", "A_SHORT", "A_FLOAT", "A_DOUBLE", "A_BOOLEAN"))
    }

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
  }

  def toString(id: AnyRef): String = Bytes.toString(id.asInstanceOf[Array[Byte]]);
}
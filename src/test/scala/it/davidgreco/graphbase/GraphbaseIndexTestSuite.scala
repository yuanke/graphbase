package it.davidgreco.graphbase

import scala.collection.JavaConversions._
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{BeforeAndAfterEach, Spec}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.Bytes
import com.tinkerpop.blueprints.pgm.{Element, Edge, Vertex, IndexableGraph}

class GraphbaseIndexTestSuite extends Spec with ShouldMatchers with BeforeAndAfterEach with EmbeddedHbase {

  val port = "21818"

  describe("A graph") {

    it("should create and remove automatic indexes") {
      val conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", "localhost")
      conf.set("hbase.zookeeper.property.clientPort", port)
      val admin = new HBaseAdmin(conf)
      val graph: IndexableGraph = new HBaseGraph(admin, "simple")

      val i1 = graph.createAutomaticIndex("idx1", classOf[Vertex], Set("FirstName", "FamilyName"))
      val indexes1 = graph.getIndices
      assert(indexes1.size == 1)

      graph.dropIndex("idx1")

      val indexes2 = graph.getIndices
      assert(indexes2.size == 0)
    }

    it("should index vertex properties") {
      val conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", "localhost")
      conf.set("hbase.zookeeper.property.clientPort", port)
      val admin = new HBaseAdmin(conf)
      val graph: IndexableGraph = new HBaseGraph(admin, "simple")

      val i1 = graph.createAutomaticIndex("idx1", classOf[Vertex], Set("FirstName", "FamilyName"))

      val v1 = graph.addVertex(null)
      v1.setProperty("FirstName", "David")
      v1.setProperty("FamilyName", "Greco")

      val v1n1 = i1.get("FirstName", "David")
      val v1n2 = i1.get("FamilyName", "Greco")
      assert(v1n1.size == 1)
      assert(v1n2.size == 1)
      assert(toString(v1n1.toBuffer.apply(0).getId) == toString(v1n2.toBuffer.apply(0).getId))
    }

    it("should index edge properties") {
      val conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", "localhost")
      conf.set("hbase.zookeeper.property.clientPort", port)
      val admin = new HBaseAdmin(conf)
      val graph: IndexableGraph = new HBaseGraph(admin, "simple")

      val i1 = graph.createAutomaticIndex("idx2", classOf[Edge], Set("Prop1", "Prop2"))

      val v1 = graph.addVertex(null)
      val v2 = graph.addVertex(null)
      val e1 = graph.addEdge(null, v1, v2, "")
      e1.setProperty("Prop1", "Val1")
      e1.setProperty("Prop2", "Val2")

      val e1n1 = i1.get("Prop1", "Val1")
      val e1n2 = i1.get("Prop2", "Val2")
      assert(e1n1.size == 1)
      assert(e1n2.size == 1)
      assert(toString(e1n1.toBuffer.apply(0).getId) == toString(e1n2.toBuffer.apply(0).getId))
    }

    it("should index edges and vertexes together") {
      val conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", "localhost")
      conf.set("hbase.zookeeper.property.clientPort", port)
      val admin = new HBaseAdmin(conf)
      val graph: IndexableGraph = new HBaseGraph(admin, "simple")

      val i1 = graph.createAutomaticIndex("idx3", classOf[Element], Set("Prop1", "Prop2", "FirstName", "FamilyName"))

      val v1 = graph.addVertex(null)
      val v2 = graph.addVertex(null)
      v1.setProperty("FirstName", "David")
      v1.setProperty("FamilyName", "Greco")

      val v1n1 = i1.get("FirstName", "David")
      val v1n2 = i1.get("FamilyName", "Greco")
      assert(v1n1.size == 1)
      assert(v1n2.size == 1)
      assert(toString(v1n1.toBuffer.apply(0).getId) == toString(v1n2.toBuffer.apply(0).getId))

      val e1 = graph.addEdge(null, v1, v2, "")
      e1.setProperty("Prop1", "Val1")
      e1.setProperty("Prop2", "Val2")

      val e1n1 = i1.get("Prop1", "Val1")
      val e1n2 = i1.get("Prop2", "Val2")
      assert(e1n1.size == 1)
      assert(e1n2.size == 1)
      assert(toString(e1n1.toBuffer.apply(0).getId) == toString(e1n2.toBuffer.apply(0).getId))
    }

  }

  def toString(id: AnyRef): String = Bytes.toString(id.asInstanceOf[Array[Byte]]);

}
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

    it("should create and use automatic indexes") {
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

      val i1 = graph.createAutomaticIndex("idx1", classOf[Vertex], Set("FirstName", "FamilyName"))

      i1.put("FirstName", "David", v1)
      i1.put("FamilyName", "Greco", v1)

      val i2 = graph.getIndex("idx1", classOf[Vertex])
      val v1n1 = i2.get("FirstName", "David")
      val v1n2 = i2.get("FamilyName", "Greco")

      assert(toString(v1.getId) == toString(v1n1.toIndexedSeq.apply(0).getId))
      assert(toString(v1.getId) == toString(v1n2.toIndexedSeq.apply(0).getId))

      val indexes = graph.getIndices
      assert(indexes.size == 1)

      val i3 = indexes.toIndexedSeq.apply(0)
      val v1n3 = i2.get("FirstName", "David")
      val v1n4 = i2.get("FamilyName", "Greco")

      assert(toString(v1.getId) == toString(v1n3.toIndexedSeq.apply(0).getId))
      assert(toString(v1.getId) == toString(v1n4.toIndexedSeq.apply(0).getId))

      i2.remove("FamilyName", "Greco", v1)
      val empty = i2.get("FamilyName", "Greco")
      assert(empty.size == 0)

      graph.dropIndex("idx1")
      val indexes1 = graph.getIndices
      assert(indexes1.size == 0)

    }

  }

  def toString(id: AnyRef): String = Bytes.toString(id.asInstanceOf[Array[Byte]]);

}
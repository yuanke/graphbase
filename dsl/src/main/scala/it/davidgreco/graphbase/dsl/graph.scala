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
import com.tinkerpop.blueprints.pgm._
import org.apache.hadoop.hbase.client.HBaseAdmin
import it.davidgreco.graphbase.blueprints.HBaseGraph

class graph(val admin: HBaseAdmin, val name: String) {

  val graph = new HBaseGraph(admin, name)

  def addVertex(): vertex = new vertex(this.graph.addVertex(null))

  def getVertex(id: AnyRef): Option[vertex] = {
    val v = graph.getVertex(id)
    if (v != null) Some(new vertex(v)) else None
  }

  def removeVertex(v: vertex): Unit = graph.removeVertex(v.vertex)

  def getEdge(id: AnyRef): Option[edge] = {
    val e = graph.getEdge(id)
    if (e != null) Some(new edge(e)) else None
  }

  def addEdge(out: vertex, in: vertex, label: String): edge = new edge(this.graph.addEdge(null, out.vertex, in.vertex, label))

  def removeEdge(e: edge): Unit = graph.removeEdge(e.edge)

  def addVertexIndex(name: String, props: Set[String]): index[Vertex] = new index[Vertex](this.graph.asInstanceOf[IndexableGraph].createAutomaticIndex(name, classOf[Vertex], props))

  def addEdgeIndex(name: String, props: Set[String]): index[Edge] = new index[Edge](this.graph.asInstanceOf[IndexableGraph].createAutomaticIndex(name, classOf[Edge], props))

  def removeIndex(name: String): graph = {
    this.graph.asInstanceOf[IndexableGraph].dropIndex(name)
    this
  }

  def unary_+(): vertex = addVertex

  def ?\/(id: AnyRef): Option[vertex] = getVertex(id)

  def -(v: vertex): Unit = removeVertex(v)

  def ?--(id: AnyRef): Option[edge] = getEdge(id)

  def +(edge: Tuple3[vertex, String, vertex]): edge = addEdge(edge._1, edge._3, edge._2)

  def -(e: edge): Unit = removeEdge(e)

  def +=|(name: String, props: List[String]): index[Vertex] = addVertexIndex(name, props.toSet)

  def +=||(name: String, props: List[String]): index[Edge] = addEdgeIndex(name, props.toSet)

  def -=|(name: String): graph = {
    removeIndex(name)
    this
  }
}

object graph {

  def apply(admin: HBaseAdmin, name: String): graph = {
    new graph(admin, name)
  }

}








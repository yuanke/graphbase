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

import collection.JavaConversions._
import com.tinkerpop.blueprints.pgm.Vertex

class vertex(val vertex: Vertex) extends element[vertex, Vertex](vertex) {

  def getInEdges: List[edge] = (for{ e <- vertex.getInEdges } yield new edge(e)).toList

  def getOutEdges: List[edge] = (for{ e <- vertex.getOutEdges } yield new edge(e)).toList

  def getInEdges(label: String): List[edge] = (for{ e <- vertex.getInEdges(label) } yield new edge(e)).toList

  def getOutEdges(label: String): List[edge] = (for{ e <- vertex.getOutEdges(label) } yield new edge(e)).toList

  def >>=<- = getInEdges

  def >>=-> = getOutEdges

}





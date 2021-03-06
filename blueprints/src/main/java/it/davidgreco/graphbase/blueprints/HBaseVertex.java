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
package it.davidgreco.graphbase.blueprints;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Index;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

class HBaseVertex implements com.tinkerpop.blueprints.pgm.Vertex {

    private HBaseGraph graph;
    private byte[] id;

    HBaseVertex() {
    }

    @Override
    public Iterable<Edge> getOutEdges() {
        try {
            Get get = new Get(id);
            Result result = graph.handle.vtable.get(get);
            if (result.isEmpty())
                return null;
            Set<Map.Entry<byte[], byte[]>> set = result.getFamilyMap(Bytes.toBytes(graph.handle.vnameOutEdges)).entrySet();
            List<Edge> outEdges = new ArrayList<Edge>();
            for (Map.Entry<byte[], byte[]> e : set) {
                HBaseEdge edge = new HBaseEdge(graph);
                edge.setId(Util.generateEdgeId(id, e.getKey()));
                edge.setOutVertex(this);
                HBaseVertex inVertex = new HBaseVertex();
                inVertex.setId(e.getValue());
                inVertex.setGraph(graph);
                edge.setInVertex(inVertex);
                String label = Bytes.toString(result.getValue(Bytes.toBytes(graph.handle.vnameEdgeProperties), Util.generateEdgePropertyId("label", e.getKey())));
                edge.setLabel(label);
                outEdges.add(edge);
            }
            return outEdges;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterable<Edge> getInEdges() {
        try {
            Get get = new Get(id);
            Result result = graph.handle.vtable.get(get);
            if (result.isEmpty())
                return null;
            Set<Map.Entry<byte[], byte[]>> set = result.getFamilyMap(Bytes.toBytes(graph.handle.vnameInEdges)).entrySet();
            List<Edge> inEdges = new ArrayList<Edge>();
            for (Map.Entry<byte[], byte[]> e : set) {
                Util.EdgeIdStruct struct = Util.getEdgeIdStruct(e.getValue());
                HBaseEdge edge = new HBaseEdge(graph);
                edge.setId(e.getValue());
                edge.setInVertex(this);
                HBaseVertex outVertex = new HBaseVertex();
                outVertex.setId(struct.vertexId);
                edge.setOutVertex(outVertex);
                Get outGet = new Get(struct.vertexId);
                Result outResult = graph.handle.vtable.get(outGet);
                String label = Bytes.toString(outResult.getValue(Bytes.toBytes(graph.handle.vnameEdgeProperties), Util.generateEdgePropertyId("label", e.getKey())));
                edge.setLabel(label);
                inEdges.add(edge);
            }
            return inEdges;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterable<Edge> getOutEdges(String label) {
        try {
            Get get = new Get(id);
            Result result = graph.handle.vtable.get(get);
            if (result.isEmpty())
                return null;
            Set<Map.Entry<byte[], byte[]>> set = result.getFamilyMap(Bytes.toBytes(graph.handle.vnameOutEdges)).entrySet();
            List<Edge> outEdges = new ArrayList<Edge>();
            for (Map.Entry<byte[], byte[]> e : set) {
                HBaseEdge edge = new HBaseEdge(graph);
                edge.setId(Util.generateEdgeId(id, e.getKey()));
                edge.setOutVertex(this);
                HBaseVertex inVertex = new HBaseVertex();
                inVertex.setId(e.getValue());
                inVertex.setGraph(graph);
                edge.setInVertex(inVertex);
                String l = Bytes.toString(result.getValue(Bytes.toBytes(graph.handle.vnameEdgeProperties), Util.generateEdgePropertyId("label", e.getKey())));
                edge.setLabel(l);
                if (l.equals(label)) {
                    outEdges.add(edge);
                }
            }
            return outEdges;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterable<Edge> getInEdges(String label) {
        try {
            Get get = new Get(id);
            Result result = graph.handle.vtable.get(get);
            if (result.isEmpty())
                return null;
            Set<Map.Entry<byte[], byte[]>> set = result.getFamilyMap(Bytes.toBytes(graph.handle.vnameInEdges)).entrySet();
            List<Edge> inEdges = new ArrayList<Edge>();
            for (Map.Entry<byte[], byte[]> e : set) {
                Util.EdgeIdStruct struct = Util.getEdgeIdStruct(e.getValue());
                HBaseEdge edge = new HBaseEdge(this.graph);
                edge.setId(e.getValue());
                edge.setInVertex(this);
                HBaseVertex outVertex = new HBaseVertex();
                outVertex.setId(struct.vertexId);
                edge.setOutVertex(outVertex);
                Get outGet = new Get(struct.vertexId);
                Result outResult = graph.handle.vtable.get(outGet);
                String l = Bytes.toString(outResult.getValue(Bytes.toBytes(graph.handle.vnameEdgeProperties), Util.generateEdgePropertyId("label", e.getKey())));
                edge.setLabel(l);
                if (l.equals(label)) {
                    inEdges.add(edge);
                }
            }
            return inEdges;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object getProperty(String key) {
        try {
            Get get = new Get(id);
            Result result = graph.handle.vtable.get(get);
            byte[] bvalue = result.getValue(Bytes.toBytes(graph.handle.vnameProperties), Bytes.toBytes(key));
            if (bvalue == null)
                return null;
            return Util.bytesToTypedObject(bvalue);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<String> getPropertyKeys() {
        try {
            Get get = new Get(id);
            Result result = graph.handle.vtable.get(get);
            NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(graph.handle.vnameProperties));
            Set<String> keys = new TreeSet<String>();
            Set<byte[]> bkeys = familyMap.keySet();
            for (byte[] bkey : bkeys) {
                if (bkey.length != 0) {
                    keys.add(Bytes.toString(bkey));
                }
            }
            return keys;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setProperty(String key, Object value) {
        try {
            byte[] bvalue = Util.typedObjectToBytes(value);
            Put put = new Put(id);
            put.add(Bytes.toBytes(graph.handle.vnameProperties), Bytes.toBytes(key), bvalue);
            boolean res = graph.handle.vtable.checkAndPut(id, Bytes.toBytes(graph.handle.vnameProperties), Bytes.toBytes(key), null, put);
            if (!res) {
                //I remove the old property from the index
                Object oldValue = this.getProperty(key);
                //Automatic indices update
                for (Index e : graph.indices.values()) {
                    e.remove(key, oldValue, this);
                }
                graph.handle.vtable.put(put);
            }
            //Automatic indices update
            for (Index e : graph.indices.values()) {
                e.put(key, value, this);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object removeProperty(String key) {
        try {
            Get get = new Get(id);
            Result result = graph.handle.vtable.get(get);
            byte[] bvalue = result.getValue(Bytes.toBytes(graph.handle.vnameProperties), Bytes.toBytes(key));
            if (bvalue == null)
                return null;
            Delete delete = new Delete(get.getRow());
            delete.deleteColumns(Bytes.toBytes(graph.handle.vnameProperties), Bytes.toBytes(key));
            Object value = Util.bytesToTypedObject(bvalue);

            //Automatic indices update
            for (Index e : graph.indices.values()) {
                e.remove(key, value, this);
            }
            //
            graph.handle.vtable.delete(delete);
            return value;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object getId() {
        return id;
    }

    void setId(byte[] id) {
        this.id = id;
    }

    void setGraph(HBaseGraph graph) {
        this.graph = graph;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        HBaseVertex rhs = (HBaseVertex) obj;
        return new EqualsBuilder()
                .append(id, rhs.id)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(99, 33)
                .append(id)
                .toHashCode();
    }

}

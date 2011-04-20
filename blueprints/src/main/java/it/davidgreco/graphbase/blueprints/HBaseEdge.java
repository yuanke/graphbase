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

import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.Vertex;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeSet;

class HBaseEdge implements com.tinkerpop.blueprints.pgm.Edge {

    private HBaseGraph graph;
    private byte[] id;
    private HBaseVertex outVertex;
    private HBaseVertex inVertex;
    private String label;

    HBaseEdge(HBaseGraph graph) {
        this.graph = graph;
    }

    @Override
    public Vertex getOutVertex() {
        return outVertex;
    }

    @Override
    public Vertex getInVertex() {
        return inVertex;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public Object getProperty(String key) {
        try {
            Util.EdgeIdStruct struct = Util.getEdgeIdStruct(id);
            Get get = new Get(struct.vertexId);
            Result result = graph.handle.vtable.get(get);
            byte[] bvalue = result.getValue(Bytes.toBytes(graph.handle.vnameEdgeProperties), Util.generateEdgePropertyId(key, struct.edgeLocalId));
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
            Util.EdgeIdStruct struct = Util.getEdgeIdStruct(id);
            Get get = new Get(struct.vertexId);
            Result result = graph.handle.vtable.get(get);
            NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(graph.handle.vnameEdgeProperties));
            Set<String> keys = new TreeSet<String>();
            Set<byte[]> bkeys = familyMap.keySet();
            for (byte[] bkey : bkeys) {
                byte[] id = Bytes.tail(bkey, 8);
                if (Bytes.equals(id, struct.edgeLocalId)) {
                    String key = Bytes.toString(Bytes.head(bkey, bkey.length - 8));
                    if (!key.equals("label"))
                        keys.add(key);
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
            Util.EdgeIdStruct struct = Util.getEdgeIdStruct(id);
            Put put = new Put(struct.vertexId);
            put.add(Bytes.toBytes(graph.handle.vnameEdgeProperties), Util.generateEdgePropertyId(key, struct.edgeLocalId), bvalue);

            //Automatic indices update
            for (Index e : graph.indices.values()) {
                e.put(key, value, this);
            }
            //
            graph.handle.vtable.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object removeProperty(String key) {
        try {
            Util.EdgeIdStruct struct = Util.getEdgeIdStruct(id);
            Get get = new Get(struct.vertexId);
            Result result = graph.handle.vtable.get(get);
            byte[] bvalue = result.getValue(Bytes.toBytes(graph.handle.vnameEdgeProperties), Util.generateEdgePropertyId(key, struct.edgeLocalId));
            if (bvalue == null)
                return null;
            Delete delete = new Delete(get.getRow());
            delete.deleteColumns(Bytes.toBytes(graph.handle.vnameEdgeProperties), Util.generateEdgePropertyId(key, struct.edgeLocalId));
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

    void setOutVertex(HBaseVertex outVertex) {
        this.outVertex = outVertex;
    }

    void setInVertex(HBaseVertex inVertex) {
        this.inVertex = inVertex;
    }

    void setLabel(String label) {
        this.label = label;
    }

}

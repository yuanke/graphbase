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

import com.tinkerpop.blueprints.pgm.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class HBaseGraph implements Graph, IndexableGraph {

    final HBaseHelper handle;
    final Map<String, Index> indices;

    public HBaseGraph(HBaseAdmin admin, String name) {
        this.handle = new HBaseHelper(admin, name);
        this.indices = new HashMap<String, Index>();
        Iterable<Index<? extends Element>> iterable = this.getIndices();
        for (Index<? extends Element> index : iterable) {
            indices.put(index.getIndexName(), index);
        }
    }

    @Override
    public Vertex addVertex(Object o) {
        try {
            byte[] id = Util.generateVertexId();
            HBaseVertex vertex = new HBaseVertex();
            vertex.setId(id);
            vertex.setGraph(this);
            Put put = new Put(id);
            put.add(Bytes.toBytes(handle.vnameProperties), null, null);
            handle.vtable.put(put);
            return vertex;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Vertex getVertex(Object id) {
        try {
            Get g = new Get((byte[]) id);
            Result result = handle.vtable.get(g);

            if (result.isEmpty())
                return null;

            HBaseVertex vertex = new HBaseVertex();
            vertex.setGraph(this);
            vertex.setId((byte[]) id);
            return vertex;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void removeVertex(Vertex vertex) {
        try {
            Delete delete = new Delete((byte[]) vertex.getId());
            handle.vtable.delete(delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterable<Vertex> getVertices() {
        throw new RuntimeException("Not supported");
    }

    @Override
    public Edge addEdge(Object o, Vertex outVertex, Vertex inVertex, String label) {
        RowLock lockOut = null;
        RowLock lockIn = null;
        try {
            Get gOut = new Get((byte[]) outVertex.getId());
            Result resultOut = handle.vtable.get(gOut);
            Get gIn = new Get((byte[]) inVertex.getId());
            Result resultIn = handle.vtable.get(gIn);
            if (!resultIn.isEmpty() && !resultOut.isEmpty()) {
                lockOut = handle.vtable.lockRow((byte[]) outVertex.getId());
                lockIn = handle.vtable.lockRow((byte[]) inVertex.getId());
                byte[] edgeLocalId = Util.generateEdgeLocalId();
                Put outPut = new Put((byte[]) outVertex.getId(), lockOut);
                outPut.add(Bytes.toBytes(handle.vnameOutEdges), edgeLocalId, (byte[]) inVertex.getId());
                outPut.add(Bytes.toBytes(handle.vnameEdgeProperties), Util.generateEdgePropertyId("label", edgeLocalId), Bytes.toBytes(label));
                byte[] edgeId = Util.generateEdgeId((byte[]) outVertex.getId(), edgeLocalId);
                handle.vtable.put(outPut);

                Put inPut = new Put((byte[]) inVertex.getId(), lockIn);
                inPut.add(Bytes.toBytes(handle.vnameInEdges), edgeLocalId, edgeId);
                handle.vtable.put(inPut);

                HBaseEdge edge = new HBaseEdge(this);
                edge.setId(edgeId);
                edge.setOutVertex((HBaseVertex) outVertex);
                edge.setInVertex((HBaseVertex) inVertex);
                edge.setLabel(label);
                return edge;
            } else {
                throw new RuntimeException("One or both vertexes don't exist");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (lockOut != null) {
                try {
                    handle.vtable.unlockRow(lockOut);
                } catch (IOException ignored) {

                }
            }
            if (lockIn != null) {
                try {
                    handle.vtable.unlockRow(lockIn);
                } catch (IOException ignored) {

                }
            }
        }
    }

    @Override
    public Edge getEdge(Object id) {
        try {
            Util.EdgeIdStruct struct = Util.getEdgeIdStruct((byte[]) id);

            byte[] outVertexId = struct.vertexId;

            Get g = new Get(struct.vertexId);
            Result result = handle.vtable.get(g);
            if (result.isEmpty())
                return null;

            byte[] inVertexId = result.getValue(Bytes.toBytes(handle.vnameOutEdges), struct.edgeLocalId);

            if (inVertexId == null) {
                return null;
            }

            String label = Bytes.toString(result.getValue(Bytes.toBytes(handle.vnameEdgeProperties), Util.generateEdgePropertyId("label", struct.edgeLocalId)));

            HBaseEdge edge = new HBaseEdge(this);
            HBaseVertex outVertex = new HBaseVertex();
            outVertex.setId(outVertexId);
            HBaseVertex inVertex = new HBaseVertex();
            inVertex.setId(inVertexId);
            edge.setId((byte[]) id);
            edge.setInVertex(inVertex);
            edge.setOutVertex(outVertex);
            edge.setLabel(label);
            return edge;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void removeEdge(Edge edge) {
        try {
            byte[] outVertexId = (byte[]) edge.getOutVertex().getId();
            byte[] inVertexId = (byte[]) edge.getInVertex().getId();
            Get gOut = new Get(outVertexId);
            Result resultOut = handle.vtable.get(gOut);
            Get gIn = new Get(inVertexId);
            Result resultIn = handle.vtable.get(gIn);
            if (!resultIn.isEmpty() && !resultOut.isEmpty()) {
                Util.EdgeIdStruct struct = Util.getEdgeIdStruct((byte[]) edge.getId());
                Delete delete = new Delete(gOut.getRow());
                delete.deleteColumns(Bytes.toBytes(handle.vnameOutEdges), struct.edgeLocalId);
                NavigableMap<byte[], byte[]> familyMap = resultOut.getFamilyMap(Bytes.toBytes(handle.vnameEdgeProperties));
                Set<byte[]> bkeys = familyMap.keySet();
                for (byte[] bkey : bkeys) {
                    byte[] id = Bytes.tail(bkey, 8);
                    if (Bytes.equals(id, struct.edgeLocalId)) {
                        delete.deleteColumns(Bytes.toBytes(handle.vnameEdgeProperties), bkey);
                    }
                }
                handle.vtable.delete(delete);
                delete = new Delete(gIn.getRow());
                delete.deleteColumns(Bytes.toBytes(handle.vnameInEdges), struct.edgeLocalId);
                handle.vtable.delete(delete);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterable<Edge> getEdges() {
        throw new RuntimeException("Not supported");
    }

    @Override
    public void clear() {
        throw new RuntimeException("Not supported");
    }

    @Override
    public void shutdown() {
        throw new RuntimeException("Not supported");
    }

    @Override
    public <T extends Element> Index<T> createManualIndex(String indexName, Class<T> indexClass) {
        throw new RuntimeException("Not supported");
    }

    @Override
    public <T extends Element> AutomaticIndex<T> createAutomaticIndex(String indexName, Class<T> indexClass, Set<String> keys) {
        ConcurrentHashMap<String, HBaseHelper.IndexTableStruct> indexTables = handle.createAutomaticIndexTables(indexName, indexClass, keys);
        HBaseIndex<T> index = new HBaseIndex<T>(this, indexName, indexClass, indexTables);
        indices.put(index.getIndexName(), index);
        return index;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Element> Index<T> getIndex(String indexName, Class<T> indexClass) {
        try {
            Get get = new Get(Bytes.toBytes(indexName));
            Result res = handle.ivtable.get(get);
            if (res.isEmpty()) {
                throw new RuntimeException("An index with this name " + indexName + " does not exist");
            }
            String clazz = Bytes.toString(res.getValue(Bytes.toBytes(handle.ivnameClass), null));
            Class c;
            if (clazz.equals(HBaseHelper.vertexClass)) {
                c = Vertex.class;
            } else if (clazz.equals(HBaseHelper.edgeClass)) {
                c = Edge.class;
            } else {
                throw new RuntimeException("Can not determine whether it is a vertex or edge class");
            }

            if (!c.isAssignableFrom(indexClass))
                throw new RuntimeException("Stored index is " + c + " and is being loaded as a " + indexClass + " index");

            ConcurrentHashMap<String, HBaseHelper.IndexTableStruct> indexTables = handle.getAutomaticIndexTables(indexName);
            HBaseIndex<T> index = new HBaseIndex<T>(this, indexName, c, indexTables);
            indices.put(index.getIndexName(), index);
            return index;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterable<Index<? extends Element>> getIndices() {
        try {
            List<Index<? extends Element>> indexes = new ArrayList<Index<? extends Element>>();
            Scan vscan = new Scan();
            ResultScanner vscanner = handle.ivtable.getScanner(vscan);
            for (Result res : vscanner) {
                String indexName = Bytes.toString(res.getRow());
                String clazz = Bytes.toString(res.getValue(Bytes.toBytes(handle.ivnameClass), null));
                Class c;
                if (clazz.equals(HBaseHelper.vertexClass)) {
                    c = Vertex.class;
                } else if (clazz.equals(HBaseHelper.edgeClass)) {
                    c = Edge.class;
                } else {
                    throw new RuntimeException("Can not determine whether it is a vertex or an edge class");
                }

                ConcurrentHashMap<String, HBaseHelper.IndexTableStruct> indexTables = handle.getAutomaticIndexTables(indexName);
                indexes.add(new HBaseIndex(this, indexName, c, indexTables));
            }
            vscanner.close();
            return indexes;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropIndex(String name) {
        handle.dropIndexTables(name);
        Iterable<Index<? extends Element>> iterable = this.getIndices();
        for (Index<? extends Element> index : iterable) {
            indices.put(index.getIndexName(), index);
        }

    }
}

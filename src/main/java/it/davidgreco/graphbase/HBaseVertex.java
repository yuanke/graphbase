package it.davidgreco.graphbase;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Index;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class HBaseVertex implements com.tinkerpop.blueprints.pgm.Vertex {

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
                HBaseEdge edge = new HBaseEdge();
                edge.setId(Util.generateEdgeId(id, e.getKey()));
                edge.setOutVertex(this);
                HBaseVertex inVertex = new HBaseVertex();
                inVertex.setId(e.getValue());
                inVertex.setGraph(graph);
                edge.setInVertex(inVertex);
                String label = Bytes.toString(result.getValue(Bytes.toBytes(graph.handle.vnameEdgeProperties), Util.generateEdgePropertyId("label", e.getKey())));
                edge.setLabel(label);
                edge.setHandle(graph.handle);
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
                HBaseEdge edge = new HBaseEdge();
                edge.setId(e.getValue());
                edge.setInVertex(this);
                HBaseVertex outVertex = new HBaseVertex();
                outVertex.setId(struct.vertexId);
                edge.setOutVertex(outVertex);
                Get outGet = new Get(struct.vertexId);
                Result outResult = graph.handle.vtable.get(outGet);
                String label = Bytes.toString(outResult.getValue(Bytes.toBytes(graph.handle.vnameEdgeProperties), Util.generateEdgePropertyId("label", e.getKey())));
                edge.setLabel(label);
                edge.setHandle(graph.handle);
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
            List outEdges = new ArrayList();
            for (Map.Entry<byte[], byte[]> e : set) {
                HBaseEdge edge = new HBaseEdge();
                edge.setId(Util.generateEdgeId(id, e.getKey()));
                edge.setOutVertex(this);
                HBaseVertex inVertex = new HBaseVertex();
                inVertex.setId(e.getValue());
                inVertex.setGraph(graph);
                edge.setInVertex(inVertex);
                String l = Bytes.toString(result.getValue(Bytes.toBytes(graph.handle.vnameEdgeProperties), Util.generateEdgePropertyId("label", e.getKey())));
                edge.setLabel(l);
                edge.setHandle(graph.handle);
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
            List inEdges = new ArrayList<Edge>();
            for (Map.Entry<byte[], byte[]> e : set) {
                Util.EdgeIdStruct struct = Util.getEdgeIdStruct(e.getValue());
                HBaseEdge edge = new HBaseEdge();
                edge.setId(e.getValue());
                edge.setInVertex(this);
                HBaseVertex outVertex = new HBaseVertex();
                outVertex.setId(struct.vertexId);
                edge.setOutVertex(outVertex);
                Get outGet = new Get(struct.vertexId);
                Result outResult = graph.handle.vtable.get(outGet);
                String l = Bytes.toString(outResult.getValue(Bytes.toBytes(graph.handle.vnameEdgeProperties), Util.generateEdgePropertyId("label", e.getKey())));
                edge.setLabel(l);
                edge.setHandle(graph.handle);
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
                keys.add(Bytes.toString(bkey));
            }
            return keys;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setProperty(String key, Object value) {
        try {
            byte[] bvalue = Util.typedObjectToBytes(value);
            Put put = new Put(id);
            put.add(Bytes.toBytes(graph.handle.vnameProperties), Bytes.toBytes(key), bvalue);

            //Automatic indees update
            List<Index> elementIndexes = graph.indices.get(HBaseHelper.elementClass);
            List<Index> vectorIndexes = graph.indices.get(HBaseHelper.vertexClass);
            for (Index e : elementIndexes) {
                e.put(key, value, this);
            }
            for (Index e : vectorIndexes) {
                e.put(key, value, this);
            }
            //
            graph.handle.vtable.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
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

            //Automatic indees update
            List<Index> elementIndexes = graph.indices.get(HBaseHelper.elementClass);
            List<Index> vectorIndexes = graph.indices.get(HBaseHelper.vertexClass);
            for (Index e : elementIndexes) {
                e.remove(key, value, this);
            }
            for (Index e : vectorIndexes) {
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

}

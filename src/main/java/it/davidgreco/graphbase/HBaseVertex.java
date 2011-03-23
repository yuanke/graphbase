package it.davidgreco.graphbase;

import com.tinkerpop.blueprints.pgm.Edge;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class HBaseVertex implements com.tinkerpop.blueprints.pgm.Vertex {

    private HbaseHelper handle;
    private byte[] id;

    HBaseVertex() {
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterable<Edge> getOutEdges() {
        try {
            Get get = new Get(id);
            Result result = handle.vtable.get(get);
            if (result.isEmpty())
                return null;
            Set<Map.Entry<byte[], byte[]>> set = result.getFamilyMap(Bytes.toBytes(handle.vnameOutEdges)).entrySet();
            List outEdges = new ArrayList();
            for (Map.Entry<byte[], byte[]> e : set) {
                HBaseEdge edge = new HBaseEdge();
                edge.setId(Util.generateEdgeId(id, e.getKey()));
                edge.setOutVertex(this);
                HBaseVertex inVertex = new HBaseVertex();
                inVertex.setId(e.getValue());
                inVertex.setHandle(handle);
                edge.setInVertex(inVertex);
                String label = Bytes.toString(result.getValue(Bytes.toBytes(handle.vnameEdgeProperties), Util.generateEdgePropertyId("label", e.getKey())));
                edge.setLabel(label);
                edge.setHandle(handle);
                outEdges.add(edge);
            }
            return outEdges;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterable<Edge> getInEdges() {
        try {
            Get get = new Get(id);
            Result result = handle.vtable.get(get);
            if (result.isEmpty())
                return null;
            Set<Map.Entry<byte[], byte[]>> set = result.getFamilyMap(Bytes.toBytes(handle.vnameInEdges)).entrySet();
            List inEdges = new ArrayList();
            for (Map.Entry<byte[], byte[]> e : set) {
                Util.EdgeIdStruct struct = Util.getEdgeIdStruct(e.getValue());
                HBaseEdge edge = new HBaseEdge();
                edge.setId(e.getValue());
                edge.setInVertex(this);
                HBaseVertex outVertex = new HBaseVertex();
                outVertex.setId(struct.vertexId);
                edge.setOutVertex(outVertex);
                Get outGet = new Get(struct.vertexId);
                Result outResult = handle.vtable.get(outGet);
                String label = Bytes.toString(outResult.getValue(Bytes.toBytes(handle.vnameEdgeProperties), Util.generateEdgePropertyId("label", e.getKey())));
                edge.setLabel(label);
                edge.setHandle(handle);
                inEdges.add(edge);
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
            Result result = handle.vtable.get(get);
            byte[] bvalue = result.getValue(Bytes.toBytes(handle.vnameProperties), Bytes.toBytes(key));
            return Util.bytesToTypedObject(bvalue);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<String> getPropertyKeys() {
        try {
            Get get = new Get(id);
            Result result = handle.vtable.get(get);
            NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(handle.vnameProperties));
            Set<String> keys = new TreeSet<String>();
            Set<byte[]> bkeys = familyMap.keySet();
            for(byte[] bkey: bkeys) {
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
            put.add(Bytes.toBytes(handle.vnameProperties), Bytes.toBytes(key), bvalue);
            handle.vtable.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object removeProperty(String s) {
        return null;
    }

    @Override
    public Object getId() {
        return id;
    }

    void setId(byte[] id) {
        this.id = id;
    }

    void setHandle(HbaseHelper handle) {
        this.handle = handle;
    }

}

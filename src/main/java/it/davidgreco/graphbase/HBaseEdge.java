package it.davidgreco.graphbase;

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

public class HBaseEdge implements com.tinkerpop.blueprints.pgm.Edge {

    private Helper handle;
    private byte[] id;
    private HBaseVertex outVertex;
    private HBaseVertex inVertex;
    private String label;

    HBaseEdge() {
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
            Result result = handle.vtable.get(get);
            byte[] bvalue = result.getValue(Bytes.toBytes(handle.vnameEdgeProperties), Util.generateEdgePropertyId(key, struct.edgeLocalId));
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
            Result result = handle.vtable.get(get);
            NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(handle.vnameEdgeProperties));
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
    public void setProperty(String key, Object value) {
        try {
            byte[] bvalue = Util.typedObjectToBytes(value);
            Util.EdgeIdStruct struct = Util.getEdgeIdStruct(id);
            Put put = new Put(struct.vertexId);
            put.add(Bytes.toBytes(handle.vnameEdgeProperties), Util.generateEdgePropertyId(key, struct.edgeLocalId), bvalue);
            handle.vtable.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object removeProperty(String key) {
        try {
            Util.EdgeIdStruct struct = Util.getEdgeIdStruct(id);
            Get get = new Get(struct.vertexId);
            Result result = handle.vtable.get(get);
            byte[] bvalue = result.getValue(Bytes.toBytes(handle.vnameEdgeProperties), Util.generateEdgePropertyId(key, struct.edgeLocalId));
            if (bvalue == null)
                return null;
            Delete delete = new Delete(get.getRow());
            delete.deleteColumns(Bytes.toBytes(handle.vnameEdgeProperties), Util.generateEdgePropertyId(key, struct.edgeLocalId));
            handle.vtable.delete(delete);
            return Util.bytesToTypedObject(bvalue);
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

    void setHandle(Helper handle) {
        this.handle = handle;
    }

}

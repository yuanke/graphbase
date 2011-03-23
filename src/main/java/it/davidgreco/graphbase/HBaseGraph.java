package it.davidgreco.graphbase;

import com.tinkerpop.blueprints.pgm.*;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseGraph implements Graph, IndexableGraph {

    private final HbaseHelper handle;

    public HBaseGraph(HBaseAdmin admin, String name) {
        this.handle = new HbaseHelper(admin, name);
    }

    @Override
    public Vertex addVertex(Object o) {
        try {
            byte[] id = Util.generateVertexId();
            HBaseVertex vertex = new HBaseVertex();
            vertex.setId(id);
            vertex.setHandle(handle);
            Put put = new Put(id);
            put.add(Bytes.toBytes(handle.vnameOutEdgeCounter), null, Bytes.toBytes(0L));
            put.add(Bytes.toBytes(handle.vnameInEdgeCounter), null, Bytes.toBytes(0L));
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
            vertex.setId((byte[]) id);
            return vertex;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void removeVertex(Vertex vertex) {
    }

    @Override
    public Iterable<Vertex> getVertices() {
        return null;
    }

    @Override
    public Edge addEdge(Object o, Vertex outVertex, Vertex inVertex, String label) {
        try {
            Get gOut = new Get((byte[]) outVertex.getId());
            Result resultOut = handle.vtable.get(gOut);
            Get gIn = new Get((byte[]) inVertex.getId());
            Result resultIn = handle.vtable.get(gIn);
            if (!resultIn.isEmpty() && !resultOut.isEmpty()) {

                long outEdgeCounter = Bytes.toLong(resultOut.getValue(Bytes.toBytes(handle.vnameOutEdgeCounter), null));
                Put outPut = new Put((byte[]) outVertex.getId());
                outPut.add(Bytes.toBytes(handle.vnameOutEdges), Bytes.toBytes(outEdgeCounter), (byte[]) inVertex.getId());
                outPut.add(Bytes.toBytes(handle.vnameEdgeProperties), Util.generateEdgePropertyId("label", outEdgeCounter), Bytes.toBytes(label));
                byte[] edgeId = Util.generateEdgeId((byte[]) outVertex.getId(), outEdgeCounter);
                outEdgeCounter++;
                outPut.add(Bytes.toBytes(handle.vnameOutEdgeCounter), null, Bytes.toBytes(outEdgeCounter));
                handle.vtable.put(outPut);

                long inEdgeCounter = Bytes.toLong(resultIn.getValue(Bytes.toBytes(handle.vnameInEdgeCounter), null));
                Put inPut = new Put((byte[]) inVertex.getId());
                inPut.add(Bytes.toBytes(handle.vnameInEdges), Bytes.toBytes(inEdgeCounter), edgeId);
                inEdgeCounter++;
                inPut.add(Bytes.toBytes(handle.vnameInEdgeCounter), null, Bytes.toBytes(inEdgeCounter));
                handle.vtable.put(inPut);

                HBaseEdge edge = new HBaseEdge();
                edge.setId(edgeId);
                edge.setOutVertex((HBaseVertex) outVertex);
                edge.setInVertex((HBaseVertex) inVertex);
                edge.setLabel(label);
                edge.setHandle(handle);
                return edge;
            } else {
                throw new RuntimeException("One or both vertexes don't exist");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Edge getEdge(Object id) {
        try {
            Util.EdgeIdStruct struct = Util.getEdgeIdStruct((byte[]) id);

            byte[] outVertexId = struct.vertexId;

            Get g = new Get((byte[]) struct.vertexId);
            Result result = handle.vtable.get(g);
            if (result.isEmpty())
                return null;

            byte[] inVertexId = result.getValue(Bytes.toBytes(handle.vnameOutEdges), struct.edgeLocalId);
            String label = Bytes.toString(result.getValue(Bytes.toBytes(handle.vnameEdgeProperties), Util.generateEdgePropertyId("label", struct.edgeLocalId)));

            HBaseEdge edge = new HBaseEdge();
            HBaseVertex outVertex = new HBaseVertex();
            outVertex.setId(outVertexId);
            HBaseVertex inVertex = new HBaseVertex();
            inVertex.setId(inVertexId);
            edge.setId((byte[]) id);
            edge.setInVertex(inVertex);
            edge.setOutVertex(outVertex);
            edge.setLabel(label);
            edge.setHandle(handle);
            return edge;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void removeEdge(Edge edge) {
    }

    @Override
    public Iterable<Edge> getEdges() {
        return null;
    }

    @Override
    public void clear() {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public <T extends Element> Index<T> createIndex(String s, Class<T> tClass, Index.Type type) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public <T extends Element> Index<T> getIndex(String s, Class<T> tClass) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Iterable<Index<? extends Element>> getIndices() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void dropIndex(String s) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}

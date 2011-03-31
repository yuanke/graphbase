package it.davidgreco.graphbase;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.Vertex;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HBaseManualIndex<T extends Element> implements Index<T> {

    final String name;
    final Class<? extends Element> indexClass;
    final HBaseGraph graph;
    final HTable indexTable;

    HBaseManualIndex(HBaseGraph graph, String name, Class<T> indexClass) {
        this.name = name;
        this.indexClass = indexClass;
        this.graph = graph;
        this.indexTable = graph.handle.createIndexTable("manual_index_" + name + "_" + indexClass.getSimpleName());
    }

    @Override
    public String getIndexName() {
        return name;
    }

    @Override
    public Class<T> getIndexClass() {
        return (Class<T>) indexClass;
    }

    @Override
    public Type getIndexType() {
        return Index.Type.MANUAL;
    }

    @Override
    public void put(String key, Object value, T element) {
        try {
            byte[] bkey = Util.generateEdgePropertyId(key, Util.typedObjectToBytes(value));
            System.out.println(Bytes.toString(bkey));
            Put put = new Put(bkey);
            put.add(Bytes.toBytes(graph.handle.elementIds), (byte[]) element.getId(), (byte[]) element.getId());
            indexTable.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterable<T> get(String key, Object value) {
        try {
            byte[] bkey = Util.generateEdgePropertyId(key, Util.typedObjectToBytes(value));
            Get get = new Get(bkey);
            Result result = indexTable.get(get);
            List<T> elements = new ArrayList<T>();
            if (!result.isEmpty()) {
                Set<Map.Entry<byte[], byte[]>> set = result.getFamilyMap(Bytes.toBytes(graph.handle.elementIds)).entrySet();
                for (Map.Entry<byte[], byte[]> e : set) {
                    if (indexClass.equals(Vertex.class)) {
                        elements.add((T) graph.getVertex(e.getValue()));
                    }
                    if(indexClass.equals((Edge.class))) {
                        elements.add((T) graph.getEdge(e.getValue()));
                    }
                }
            }
            return elements;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void remove(String key, Object value, T element) {
        try {
            byte[] bkey = Util.generateEdgePropertyId(key, Util.typedObjectToBytes(value));
            Delete del = new Delete(bkey);
            del.deleteColumns(Bytes.toBytes(graph.handle.elementIds), (byte[]) element.getId());
            indexTable.delete(del);
      } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}

package it.davidgreco.graphbase;

import com.tinkerpop.blueprints.pgm.*;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class HBaseIndex<T extends Element> implements AutomaticIndex<T> {

    private final HBaseGraph graph;
    private final String name;
    private final Class<T> indexClass;
    private final ConcurrentHashMap<String, HTable> indexTables;

    HBaseIndex(HBaseGraph graph, String name, Class<T> indexClass, ConcurrentHashMap<String, HTable> indexTables) {
        this.graph = graph;
        this.name = name;
        this.indexClass = indexClass;
        this.indexTables = indexTables;
    }

    @Override
    public String getIndexName() {
        return name;
    }

    @Override
    public Class<T> getIndexClass() {
        return indexClass;
    }

    @Override
    public Type getIndexType() {
        return Index.Type.AUTOMATIC;
    }

    @Override
    public void put(String key, Object value, T element) {
        try {
            Put put = new Put(Util.typedObjectToBytes(value));
            put.add(Bytes.toBytes(graph.handle.getIndexTableColumnName(name, this.indexClass, key)), (byte[]) element.getId(), (byte[]) element.getId());
            HTable table = indexTables.get(key);
            table.put(put);
        } catch (IOException e) {
            new RuntimeException(e);
        }
    }

    @Override
    public Iterable<T> get(String key, Object value) {
        try {
            Get get = new Get(Util.typedObjectToBytes(value));
            HTable table = indexTables.get(key);
            List<T> elements = new ArrayList<T>();
            if (table == null) {
                return elements;
            }
            Result result = table.get(get);
            if (!result.isEmpty()) {
                Set<Map.Entry<byte[], byte[]>> set = result.getFamilyMap(Bytes.toBytes(graph.handle.getIndexTableColumnName(name, this.indexClass, key))).entrySet();
                for (Map.Entry<byte[], byte[]> e : set) {
                    if (indexClass.equals(Vertex.class)) {
                        elements.add((T) graph.getVertex(e.getValue()));
                    }
                    if (indexClass.equals((Edge.class))) {
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
    public void remove(String s, Object o, T t) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Set<String> getAutoIndexKeys() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

}

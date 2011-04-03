package it.davidgreco.graphbase;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.Vertex;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Helper {

    final HBaseAdmin admin;
    final String vname;
    final String ivname;
    final String ivnameProperties;
    final String ivnameClass;
    final String vnameProperties;
    final String vnameOutEdges;
    final String vnameInEdges;
    final String vnameEdgeProperties;
    final String indexSepString = "-";

    static final short elementClass = 0;
    static final short vertexClass = 1;
    static final short edgeClass = 2;

    HTable vtable;
    HTable ivtable;

    Helper(HBaseAdmin admin, String name) {
        this.admin = admin;
        this.vname = name;
        this.ivname = name + "_indexes";
        this.ivnameProperties = ivname + "_properties";
        this.ivnameClass = ivname + "_class";
        this.vnameProperties = vname + "_properties";
        this.vnameOutEdges = vname + "_outEdges";
        this.vnameInEdges = vname + "_inEdges";
        this.vnameEdgeProperties = vname + "edge_properties";
        try {
            if (!admin.tableExists(vname)) {
                admin.createTable(new HTableDescriptor(vname));
                admin.disableTable(vname);
                admin.addColumn(vname, new HColumnDescriptor(vnameProperties));
                admin.addColumn(vname, new HColumnDescriptor(vnameOutEdges));
                admin.addColumn(vname, new HColumnDescriptor(vnameInEdges));
                admin.addColumn(vname, new HColumnDescriptor(vnameEdgeProperties));
                admin.enableTable(vname);
            }
            this.vtable = new HTable(admin.getConfiguration(), vname);

            if (!admin.tableExists(ivname)) {
                admin.createTable(new HTableDescriptor(ivname));
                admin.disableTable(ivname);
                admin.addColumn(ivname, new HColumnDescriptor(ivnameProperties));
                admin.addColumn(ivname, new HColumnDescriptor(ivnameClass));
                admin.enableTable(ivname);
            }
            this.ivtable = new HTable(admin.getConfiguration(), ivname);
        } catch (MasterNotRunningException e) {
            throw new RuntimeException(e);
        } catch (ZooKeeperConnectionException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    <T extends Element> String getIndexTableColumnName(String name, String key) {
        return "index" + indexSepString + name + indexSepString + key + indexSepString + "indexes";
    }

    <T extends Element> short getClass(Class<T> indexClass) {
        if (indexClass.equals(Element.class)) {
            return elementClass;
        } else if (indexClass.equals(Vertex.class)) {
            return vertexClass;
        } else if (indexClass.equals(Edge.class)) {
            return edgeClass;
        } else {
            throw new RuntimeException("indexClass not supported");
        }

    }

    <T extends Element> Class<T> getClass(short ic) {
        switch (ic) {
            case elementClass:
                return (Class<T>) Element.class;
            case vertexClass:
                return (Class<T>) Vertex.class;
            case edgeClass:
                return (Class<T>) Edge.class;
            default:
                throw new RuntimeException("indexClass not supported");
        }
    }


    <T extends Element> ConcurrentHashMap<String, IndexTableStruct> createAutomaticIndexTables(String name, Class<T> indexClass, Set<String> keys) {
        ConcurrentHashMap<String, IndexTableStruct> indexTables = new ConcurrentHashMap<String, IndexTableStruct>();
        try {
            Get vget = new Get(Bytes.toBytes(name));
            Result vresult = ivtable.get(vget);
            if (!vresult.isEmpty()) {
                throw new RuntimeException("An index with this name already exists");
            }
            Put put = new Put(Bytes.toBytes(name));
            for (String key : keys) {
                String tname = "index" + indexSepString + name + indexSepString + key;
                put.add(Bytes.toBytes(ivnameProperties), Bytes.toBytes(key), Bytes.toBytes(tname));
                put.add(Bytes.toBytes(ivnameClass), null, Bytes.toBytes(getClass(indexClass)));
                String tcolname = getIndexTableColumnName(name, key);
                if (!admin.tableExists(tname)) {
                    admin.createTable(new HTableDescriptor(tname));
                    admin.disableTable(tname);
                    admin.addColumn(tname, new HColumnDescriptor(tcolname));
                    admin.enableTable(tname);
                } else {
                    throw new RuntimeException("Internal error"); //todo better error message
                }
                IndexTableStruct struct = new IndexTableStruct();
                struct.indexClass = getClass(indexClass);
                struct.indexColumnName = tcolname;
                struct.indexTable = new HTable(admin.getConfiguration(), tname);
                indexTables.put(key, struct);
            }
            ivtable.put(put);
            return indexTables;
        } catch (MasterNotRunningException e) {
            throw new RuntimeException(e);
        } catch (ZooKeeperConnectionException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    <T extends Element> ConcurrentHashMap<String, IndexTableStruct> getAutomaticIndexTables(String name, Class<T> indexClass) {
        ConcurrentHashMap<String, IndexTableStruct> indexTables = new ConcurrentHashMap<String, IndexTableStruct>();
        try {
            Get get = new Get(Bytes.toBytes(name));
            Result result = ivtable.get(get);
            if (result.isEmpty()) {
                throw new RuntimeException("An index with this name does not exist");
            }
            NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(ivnameProperties));
            Set<Map.Entry<byte[], byte[]>> entrySet = familyMap.entrySet();
            for (Map.Entry<byte[], byte[]> e : entrySet) {
                String key = Bytes.toString(e.getKey());
                String tname = Bytes.toString(e.getValue());
                IndexTableStruct struct = new IndexTableStruct();
                struct.indexClass = getClass(indexClass);
                struct.indexColumnName = getIndexTableColumnName(name, key);
                struct.indexTable = new HTable(admin.getConfiguration(), tname);
                indexTables.put(key, struct);
            }
            return indexTables;
        } catch (MasterNotRunningException e) {
            throw new RuntimeException(e);
        } catch (ZooKeeperConnectionException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void dropIndexTables(String name) {
        try {
            Get vget = new Get(Bytes.toBytes(name));
            Result vresult = ivtable.get(vget);
            if (!vresult.isEmpty()) {
                NavigableMap<byte[], byte[]> familyMap = vresult.getFamilyMap(Bytes.toBytes(ivnameProperties));
                Set<Map.Entry<byte[], byte[]>> entrySet = familyMap.entrySet();
                for (Map.Entry<byte[], byte[]> e : entrySet) {
                    String tname = Bytes.toString(e.getValue());
                    admin.disableTable(tname);
                    admin.deleteTable(tname);
                }
                Delete del = new Delete(vget.getRow());
                ivtable.delete(del);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static class IndexTableStruct {
        short indexClass;
        String indexColumnName;
        HTable indexTable;
    }

}

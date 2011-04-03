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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class HBaseHelper {

    final HBaseAdmin admin;
    final String vname;
    final String ivname;
    final String ivnameProperties;
    final String iename;
    final String ienameProperties;
    final String vnameProperties;
    final String vnameOutEdges;
    final String vnameInEdges;
    final String vnameEdgeProperties;
    final String elementIds = "elementIds";
    final String indexSepString = "-";
    final String indexEKind = "e";
    final String indexVKind = "v";
    HTable vtable;
    HTable ivtable;
    HTable ietable;

    HBaseHelper(HBaseAdmin admin, String name) {
        this.admin = admin;
        this.vname = name;
        this.ivname = name + "vertex_indexes";
        this.ivnameProperties = ivname + "_properties";
        this.iename = name + "edge_indexes";
        this.ienameProperties = iename + "_properties";
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
                admin.enableTable(ivname);
            }
            this.ivtable = new HTable(admin.getConfiguration(), ivname);

            if (!admin.tableExists(iename)) {
                admin.createTable(new HTableDescriptor(iename));
                admin.disableTable(iename);
                admin.addColumn(iename, new HColumnDescriptor(ienameProperties));
                admin.enableTable(iename);
            }
            this.ietable = new HTable(admin.getConfiguration(), iename);
        } catch (MasterNotRunningException e) {
            throw new RuntimeException(e);
        } catch (ZooKeeperConnectionException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    <T extends Element> String getIndexTableColumnName(String name, Class<T> indexClass, String key) {
        if (indexClass.equals(Vertex.class)) {
            return "vertex" + indexSepString + name + indexSepString + key + indexSepString + "indexes";
        }
        if (indexClass.equals(Edge.class)) {
            return "edge" + indexSepString + name + indexSepString + key + indexSepString + "indexes";
        }
        return null;
    }

    <T extends Element> ConcurrentHashMap<String, HTable> createAutomaticIndexTables(String name, Class<T> indexClass, Set<String> keys) {
        ConcurrentHashMap indexTables = new ConcurrentHashMap<String, HTable>();
        try {
            if (indexClass.equals(Vertex.class)) {
                Get get = new Get(Bytes.toBytes(name));
                Result result = ivtable.get(get);
                if (!result.isEmpty()) {
                    throw new RuntimeException("An index with this name already exists");
                }
                Put put = new Put(Bytes.toBytes(name));
                for (String key : keys) {
                    String tname = "vertex" + indexSepString + name + indexSepString + key;
                    put.add(Bytes.toBytes(ivnameProperties), Bytes.toBytes(key), Bytes.toBytes(tname));
                    String tcolname = getIndexTableColumnName(name, indexClass, key);
                    if (!admin.tableExists(tname)) {
                        admin.createTable(new HTableDescriptor(tname));
                        admin.disableTable(tname);
                        admin.addColumn(tname, new HColumnDescriptor(tcolname));
                        admin.enableTable(tname);
                    } else {
                        throw new RuntimeException("Internal error"); //todo better error message
                    }
                    indexTables.put(key, new HTable(admin.getConfiguration(), tname));
                }
                ivtable.put(put);
            } else if (indexClass.equals(Edge.class)) {
                Get get = new Get(Bytes.toBytes(name));
                Result result = ietable.get(get);
                if (!result.isEmpty()) {
                    throw new RuntimeException("An index with this name already exists");
                }
                Put put = new Put(Bytes.toBytes(name));
                for (String key : keys) {
                    String tname = "edge" + indexSepString + name + indexSepString + key;
                    put.add(Bytes.toBytes(ienameProperties), Bytes.toBytes(key), Bytes.toBytes(tname));
                    String tcolname = getIndexTableColumnName(name, indexClass, key);
                    if (!admin.tableExists(tname)) {
                        admin.createTable(new HTableDescriptor(tname));
                        admin.disableTable(tname);
                        admin.addColumn(tname, new HColumnDescriptor(tcolname));
                        admin.enableTable(tname);
                    } else {
                        throw new RuntimeException("Internal error"); //todo better error message
                    }
                    indexTables.put(key, new HTable(admin.getConfiguration(), tname));
                }
                ietable.put(put);
            } else {
                throw new RuntimeException("indexClass not supported");
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

    <T extends Element> ConcurrentHashMap<String, HTable> getAutomaticIndexTables(String name, Class<T> indexClass) {
        ConcurrentHashMap indexTables = new ConcurrentHashMap<String, HTable>();
        try {
            if (indexClass.equals(Vertex.class)) {
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
                    indexTables.put(key, new HTable(admin.getConfiguration(), tname));
                }
            } else if (indexClass.equals(Edge.class)) {
                Get get = new Get(Bytes.toBytes(name));
                Result result = ietable.get(get);
                if (result.isEmpty()) {
                    throw new RuntimeException("An index with this name does not exist");
                }
                NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(ienameProperties));
                Set<Map.Entry<byte[], byte[]>> entrySet = familyMap.entrySet();
                for (Map.Entry<byte[], byte[]> e : entrySet) {
                    String key = Bytes.toString(e.getKey());
                    String tname = Bytes.toString(e.getValue());
                    indexTables.put(key, new HTable(admin.getConfiguration(), tname));
                }
            } else {
                throw new RuntimeException("indexClass not supported");
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

}

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

class HBaseHelper {

    private final HBaseAdmin admin;
    final String ivnameClass;
    private final String ivnameProperties;
    final String vnameProperties;
    final String vnameOutEdges;
    final String vnameInEdges;
    final String vnameEdgeProperties;

    private static final String separator = ".-.";
    static final String vertexClass = "vertex";
    static final String edgeClass = "edge";

    HTable vtable;
    HTable ivtable;

    HBaseHelper(HBaseAdmin admin, String name) {
        this.admin = admin;
        String vname = name;
        String ivname = name + "_indexes";
        this.ivnameClass = ivname + "_class";
        this.ivnameProperties = ivname + "_properties";
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
                admin.addColumn(ivname, new HColumnDescriptor(ivnameClass));
                admin.addColumn(ivname, new HColumnDescriptor(ivnameProperties));
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

    String getIndexTableName(String name, String key) {
        return "index" + separator + name + separator + key;
    }

    String getIndexTableColumnNameIndexes(String name, String key) {
        return "index" + separator + name + separator + key + separator + "indexes";
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
                String tname = getIndexTableName(name, key);
                String tcolnameIndexes = getIndexTableColumnNameIndexes(name, key);
                if (!admin.tableExists(tname)) {
                    admin.createTable(new HTableDescriptor(tname));
                    admin.disableTable(tname);
                    admin.addColumn(tname, new HColumnDescriptor(tcolnameIndexes));
                    admin.enableTable(tname);
                } else {
                    throw new RuntimeException("Internal error"); //todo better error message
                }
                IndexTableStruct struct = new IndexTableStruct();
                struct.indexColumnNameIndexes = tcolnameIndexes;
                struct.indexTable = new HTable(admin.getConfiguration(), tname);
                String c;
                if (Vertex.class.isAssignableFrom(indexClass))
                    c = vertexClass;
                else
                    c = edgeClass;
                put.add(Bytes.toBytes(ivnameClass), null, Bytes.toBytes(c));
                put.add(Bytes.toBytes(ivnameProperties), Bytes.toBytes(key), Bytes.toBytes(tname));
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

    <T extends Element> ConcurrentHashMap<String, IndexTableStruct> getAutomaticIndexTables(String name) {
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
                struct.indexColumnNameIndexes = getIndexTableColumnNameIndexes(name, key);
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
        String indexColumnNameIndexes;
        HTable indexTable;
    }

}

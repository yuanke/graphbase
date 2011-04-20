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

import com.tinkerpop.blueprints.pgm.AutomaticIndex;
import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.Vertex;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

class HBaseIndex<T extends Element> implements AutomaticIndex<T> {

    private final HBaseGraph graph;
    private final String name;
    private final Class<T> indexClass;
    private final Map<String, HBaseHelper.IndexTableStruct> indexTables;

    HBaseIndex(HBaseGraph graph, String name, Class<T> indexClass, Map<String, HBaseHelper.IndexTableStruct> indexTables) {
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
            if (this.indexClass.isAssignableFrom(element.getClass())) {
                HBaseHelper.IndexTableStruct struct = indexTables.get(key);
                if (struct == null) {
                    return;
                }
                Put put = new Put(Util.typedObjectToBytes(value));
                put.add(Bytes.toBytes(struct.indexColumnNameIndexes), (byte[]) element.getId(), (byte[]) element.getId());
                struct.indexTable.put(put);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterable<T> get(String key, Object value) {
        try {
            HBaseHelper.IndexTableStruct struct = indexTables.get(key);
            List<T> elements = new ArrayList<T>();
            if (struct == null) {
                return elements;
            }
            Get get = new Get(Util.typedObjectToBytes(value));
            Result result = struct.indexTable.get(get);
            if (!result.isEmpty()) {
                Set<Map.Entry<byte[], byte[]>> set = result.getFamilyMap(Bytes.toBytes(struct.indexColumnNameIndexes)).entrySet();
                for (Map.Entry<byte[], byte[]> e : set) {
                    if (Vertex.class.isAssignableFrom(this.indexClass)) {
                        elements.add((T) graph.getVertex(e.getValue()));
                    } else {
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
    public long count(String s, Object o) {
        throw new RuntimeException("Not supported");
    }

    @Override
    public void remove(String key, Object value, T element) {
        try {
            if (this.indexClass.isAssignableFrom(element.getClass())) {
                HBaseHelper.IndexTableStruct struct = indexTables.get(key);
                if (struct == null) {
                    return;
                }
                Delete del = new Delete(Util.typedObjectToBytes(value));
                del.deleteColumns(Bytes.toBytes(struct.indexColumnNameIndexes), (byte[]) element.getId());
                struct.indexTable.delete(del);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<String> getAutoIndexKeys() {
        return indexTables.keySet();
    }

}

package it.davidgreco.graphbase;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.Vertex;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseHelper {

    final HBaseAdmin admin;
    final String vname;
    final String vnameProperties;
    final String vnameOutEdges;
    final String vnameInEdges;
    final String vnameEdgeProperties;
    final String elementIds = "elementIds";
    final String indexSepString = "-";
    final String indexEKind = "e";
    final String indexVKind = "v";
    HTable vtable;

    public HbaseHelper(HBaseAdmin admin, String name) {
        this.admin = admin;
        this.vname = name;
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
        } catch (MasterNotRunningException e) {
            throw new RuntimeException(e);
        } catch (ZooKeeperConnectionException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public <T extends Element> HTable createIndexTable(String indexName, Class<T> indexClass) {
        try {
            if (!doesNotExist(indexName, indexClass))
                throw new RuntimeException("An index with this name " + indexName + " already exists");
            String tableName = getIndexTableName(indexName, indexClass);
            if (!admin.tableExists(tableName)) {
                admin.createTable(new HTableDescriptor(tableName));
                admin.disableTable(tableName);
                admin.addColumn(tableName, new HColumnDescriptor(elementIds));
                admin.enableTable(tableName);
            }
            return new HTable(admin.getConfiguration(), tableName);
        } catch (MasterNotRunningException e) {
            throw new RuntimeException(e);
        } catch (ZooKeeperConnectionException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public <T extends Element> void dropIndexTable(String indexName) {
        try {
            String tableName = null;
            HTableDescriptor[] tables = admin.listTables();
            for (int i = 0; i < tables.length; ++i) {
                String tname = tables[i].getNameAsString();
                if (tname.startsWith(getIndexTableName(indexName, indexVKind)) || tname.startsWith(getIndexTableName(indexName, indexEKind))) {
                    tableName = tname;
                    break;
                }
            }

            if (tableName != null) {
                if (admin.tableExists(tableName)) {
                    admin.disableTable(tableName);
                    admin.deleteTable(tableName);
                }
            }
        } catch (MasterNotRunningException e) {
            throw new RuntimeException(e);
        } catch (ZooKeeperConnectionException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Iterable<String> getIndexNames() {
        try {
            List<String> indexNames = new ArrayList<String>();
            HTableDescriptor[] tables = admin.listTables();
            for (int i = 0; i < tables.length; ++i) {
                String tname = tables[i].getNameAsString();
                if (tname.startsWith("index_" + vname)) {
                    indexNames.add(tname);
                }
            }
            return indexNames;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private String getIndexTableName(String indexName, String kind) {
       return "index" + indexSepString + vname + indexSepString + kind + indexSepString + indexName;
    }

    private <T extends Element> String getIndexTableName(String indexName, Class<T> indexClass) {
        String kind = null;
        if (indexClass.equals(Vertex.class)) {
            kind = indexVKind;
        } else if (indexClass.equals(Edge.class)) {
            kind = indexEKind;
        }
        if (kind == null) {
            throw new RuntimeException("indexClass not supported");
        }
        return getIndexTableName(indexName, kind);
    }

    private <T extends Element> boolean doesNotExist(String indexName, Class<T> indexClass) {
        try {
            String kind = null;
            if (indexClass.equals(Vertex.class)) {
                HTableDescriptor[] tables = admin.listTables();
                for (int i = 0; i < tables.length; ++i) {
                    String tableName = tables[i].getNameAsString();
                    if (tableName.startsWith(getIndexTableName(indexName, indexEKind))) {
                        return false;
                    }
                }
            } else if (indexClass.equals(Edge.class)) {
                HTableDescriptor[] tables = admin.listTables();
                for (int i = 0; i < tables.length; ++i) {
                    String tableName = tables[i].getNameAsString();
                    if (tableName.startsWith(getIndexTableName(indexName, indexVKind))) {
                        return false;
                    }
                }
            }
            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}

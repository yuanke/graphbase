package it.davidgreco.graphbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

public class HbaseHelper {

    final HBaseAdmin admin;
    final String vname;
    final String vnameProperties;
    final String vnameOutEdges;
    final String vnameInEdges;
    final String vnameEdgeProperties;
    final String elementIds = "elementIds";
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

    public HTable createIndexTable(String indexName) {
        try {
            String tableName = vname + "_" + indexName;
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

    public void dropIndexTable(String indexName) {
        try {
            String tableName = vname + "_" + indexName;
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
        } catch (MasterNotRunningException e) {
            throw new RuntimeException(e);
        } catch (ZooKeeperConnectionException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}

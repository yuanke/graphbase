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
    final String vnameOutEdgeCounter;
    final String vnameOutEdges;
    final String vnameInEdgeCounter;
    final String vnameInEdges;
    final String vnameEdgeProperties;
    HTable vtable;

    public HbaseHelper(HBaseAdmin admin, String name) {
        this.admin = admin;
        this.vname = name;
        this.vnameProperties     = vname + "_properties";
        this.vnameOutEdgeCounter = vname + "_OutEdgeCounter";
        this.vnameOutEdges       = vname + "_outEdges";
        this.vnameInEdgeCounter  = vname + "_InEdgeCounter";
        this.vnameInEdges        = vname + "_inEdges";
        this.vnameEdgeProperties = vname + "edge_properties";
        try {
            if (!admin.tableExists(vname)) {
                admin.createTable(new HTableDescriptor(vname));
                admin.disableTable(vname);
                admin.addColumn(vname, new HColumnDescriptor(vnameProperties));
                admin.addColumn(vname, new HColumnDescriptor(vnameOutEdgeCounter));
                admin.addColumn(vname, new HColumnDescriptor(vnameOutEdges));
                admin.addColumn(vname, new HColumnDescriptor(vnameInEdgeCounter));
                admin.addColumn(vname, new HColumnDescriptor(vnameInEdges));
                admin.addColumn(vname, new HColumnDescriptor(vnameEdgeProperties));
                admin.enableTable(vname);
            }
            this.vtable = new HTable(admin.getConfiguration(), vname);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

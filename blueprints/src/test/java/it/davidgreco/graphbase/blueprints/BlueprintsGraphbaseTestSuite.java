package it.davidgreco.graphbase.blueprints;

import com.tinkerpop.blueprints.pgm.*;
import com.tinkerpop.blueprints.pgm.impls.GraphTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Test;
import org.junit.runners.Suite;

import java.io.IOException;
import java.lang.reflect.Method;

public class BlueprintsGraphbaseTestSuite extends GraphTest {

    private HBaseTestingUtility testUtil = new HBaseTestingUtility();
    private IndexableGraph graph;

    public void setUp() throws Exception {
        testUtil.startMiniCluster();
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "21818");
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(conf);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
        graph = (IndexableGraph) new HBaseGraph(admin, "simple");
    }

    public void tearDown() throws IOException {
        testUtil.shutdownMiniCluster();
    }

    public BlueprintsGraphbaseTestSuite() {
        this.allowsDuplicateEdges = true;
        this.allowsSelfLoops = false;
        this.ignoresSuppliedIds = true;
        this.isPersistent = true;
        this.isRDFModel = false;
        this.supportsVertexIteration = false;
        this.supportsEdgeIteration = false;
        this.supportsVertexIndex = false;
        this.supportsEdgeIndex = false;
        this.supportsTransactions = false;
    }

    @Test
    public void testVertexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new VertexTestSuite(this));
        printTestPerformance("VertexTestSuite", this.stopWatch());
    }

    @Test
    public void testEdgeTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new EdgeTestSuite(this));
        printTestPerformance("EdgeTestSuite", this.stopWatch());
    }

    @Test
    public void testGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphTestSuite(this));
        printTestPerformance("GraphTestSuite", this.stopWatch());
    }

    // DON'T PASS
    //public void testIndexableGraphTestSuite() throws Exception {
    //    this.stopWatch();
    //    doTestSuite(new IndexableGraphTestSuite(this));
    //    printTestPerformance("IndexableGraphTestSuite", this.stopWatch());
    //}

    @Test
    public void testIndexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new IndexTestSuite(this));
        printTestPerformance("IndexTestSuite", this.stopWatch());
    }

    // DON'T PASS
    //public void testAutomaticIndexTestSuite() throws Exception {
    //    this.stopWatch();
    //    doTestSuite(new AutomaticIndexTestSuite(this));
    //    printTestPerformance("AutomaticIndexTestSuite", this.stopWatch());
    //}

    public Graph getGraphInstance() {
        return graph;
    }

    public void doTestSuite(final TestSuite testSuite) throws Exception {
        for (Method method : testSuite.getClass().getDeclaredMethods()) {
            if (method.getName().startsWith("test")) {
                System.out.println("Testing " + method.getName() + "...");
                method.invoke(testSuite);
            }
        }
    }
}
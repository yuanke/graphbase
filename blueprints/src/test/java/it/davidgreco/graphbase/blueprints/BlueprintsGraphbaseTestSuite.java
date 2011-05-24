package it.davidgreco.graphbase.blueprints;

import com.tinkerpop.blueprints.pgm.*;
import com.tinkerpop.blueprints.pgm.impls.GraphTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;

public class BlueprintsGraphbaseTestSuite extends GraphTest {

    private HBaseTestingUtility testUtil = new HBaseTestingUtility();
    private IndexableGraph graph;

    @BeforeClass
    public void setUp() throws Exception {
        testUtil.startMiniCluster();
        graph = (IndexableGraph) new HBaseGraph("localhost", "21818", "simple");
    }

    @AfterClass
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
        this.supportsVertexIndex = true;
        this.supportsEdgeIndex = true;
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

    @Test
    public void testAutomaticIndexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new AutomaticIndexTestSuite(this));
        printTestPerformance("AutomaticIndexTestSuite", this.stopWatch());
    }

    public Graph getGraphInstance() {
        return graph;
    }

    public void doTestSuite(final TestSuite testSuite) throws Exception {
        for (Method method : testSuite.getClass().getDeclaredMethods()) {
            if (method.getName().startsWith("test") &&
                    //Index tests to be excluded because I don't support the creation of an automatic index for all the properties
                    (!method.getName().equals("testEdgeLabelIndexing") &&
                            !method.getName().equals("testAutoIndexPutGetRemoveVertex") &&
                            !method.getName().equals("testAutoIndexPutGetRemoveEdge") &&
                            !method.getName().equals("testAutomaticIndexKeysPersistent"))) {
                System.out.println("Testing " + method.getName() + "...");
                method.invoke(testSuite);
                if (method.getName().equals("testAutoIndexKeyManagement")) {
                    graph.dropIndex("test");
                }
                if (method.getName().equals("testAutoIndexSpecificKeysVertex")) {
                    graph.dropIndex("test");
                }
            }
        }
    }
}
package it.davidgreco.graphbase

import org.scalatest.{Suite, BeforeAndAfterAll}
import org.apache.hadoop.hbase.HBaseTestingUtility
trait EmbeddedHbase extends BeforeAndAfterAll {
  this: Suite =>

  val testUtil = new HBaseTestingUtility

  override protected def beforeAll(): Unit = {
    testUtil.startMiniCluster
  }

  override protected def afterAll(): Unit = {
    testUtil.shutdownMiniCluster
  }
}

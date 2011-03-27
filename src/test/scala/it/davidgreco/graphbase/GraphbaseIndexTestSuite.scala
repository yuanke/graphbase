package it.davidgreco.graphbase

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{BeforeAndAfterEach, Spec}

class GraphbaseIndexTestSuite extends Spec with ShouldMatchers with BeforeAndAfterEach with EmbeddedHbase {

  val port = "21818"

  describe("A graph") {

    it("should create manual indexes") {

    }

  }
}
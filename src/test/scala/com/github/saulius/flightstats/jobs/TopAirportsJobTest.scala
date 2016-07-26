package com.github.saulius.flightstats.jobs

import com.github.saulius.flightstats.SharedSQLContext
import org.scalatest.{FunSpec, Matchers}

class TopAirportsJobTest extends FunSpec with Matchers with SharedSQLContext {
  describe("TopAirportsJobTest.process") {
    import sqlContext.implicits._

    it("returns counted flights for each airport") {
      val testDF = Seq(("ORD", "ATL"),
                       ("ORD", "DFW"),
                       ("ATL", "ORD")).toDF("Origin", "Dest")

      val job = new TopAirportsJob(sqlContext)

      job.process(testDF).collect.map { row =>
        (row.getAs[String]("airport"), row.getAs[Integer]("flight_count"))
      } should contain theSameElementsAs Seq(("ORD", 3), ("ATL", 2), ("DFW", 1))
    }
  }
}

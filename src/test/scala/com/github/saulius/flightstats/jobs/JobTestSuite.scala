package com.github.saulius.flightstats.jobs

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSpec, Matchers, BeforeAndAfterAll}

class JobTestSuite extends FunSpec with Matchers with BeforeAndAfterAll {
  var _sparkSession: SparkSession = null
  lazy val sparkSession = _sparkSession

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    _sparkSession = SparkSession.builder
      .master("local")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try {
      _sparkSession.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  import sparkSession.implicits._

  describe("TopAirportsJob.process") {
    it("returns counted flights for each airport") {
      val testDF = Seq(("ORD", "ATL"),
                       ("ORD", "DFW"),
                       ("ATL", "ORD")).toDF("Origin", "Dest")

      val job = new TopAirportsJob(sparkSession)

      job.process(testDF).collect.map { row =>
        (row.getAs[String]("airport"), row.getAs[Integer]("flight_count"))
      } should contain theSameElementsAs Seq(("ORD", 3), ("ATL", 2), ("DFW", 1))
    }
  }

  describe("TopDelayedLinksJob.process") {
    it("returns top delayed links by arrival delay time") {
      val testDF = Seq(
        ("ORD", "ATL", 10, 2016, 1, 1),
        ("ORD", "ATL", 80, 2016, 4, 5),
        ("ATL", "DFW", 20, 2015, 2, 3),
        ("ATL", "ORD", -10, 2013,8, 9)  // filtered out, negative arrival delay
      ).toDF("Origin", "Dest", "ArrDelay", "Year", "Month", "DayofMonth")

      val job = new TopDelayedLinksJob(sparkSession)

      val expectedResult = Seq(("ORD -> ATL", 90, "2016-01-01", "2016-04-05", 2),
                               ("ATL -> DFW", 20, "2015-02-03", "2015-02-03", 1))

      job.process(testDF).collect.map { row =>
        (
          row.getAs[String]("link"),
          row.getAs[Integer]("combined_delay"),
          row.getAs[String]("earliest_date"),
          row.getAs[String]("latest_date"),
          row.getAs[Integer]("number_of_links")
        )
      } should contain theSameElementsAs expectedResult
    }
  }

  describe("ArrivalDelayPredictionJob.process") {
    it("returns probabilities of being late split by route, day of week and departure time block") {
      val testDF = Seq(
        ("ORD", "ATL", 20, 1, 200),
        ("ORD", "ATL", 0,  1, 300),
        ("ORD", "ATL", 0,  1, 500),
        ("ATL", "DFW", 5,  2, 2100)
      ).toDF("Origin", "Dest", "ArrDelay", "DayOfWeek", "DepTime")

      val job = new ArrivalDelayPredictionJob(sparkSession)

      val expectedResult = Seq(("ORD -> ATL", 1, 0, 50.0, 0.0, 50.0),
                               ("ORD -> ATL", 1, 1, 100.0, 0.0, 0.0),
                               ("ATL -> DFW", 2, 5, 0.0, 100.0, 0.0))

      job.process(testDF).collect.map { row =>
        (
          row.getAs[String]("route"),
          row.getAs[Integer]("day_of_week"),
          row.getAs[Integer]("departure_time_block"),
          row.getAs[Double]("p_on_time"),
          row.getAs[Double]("p_less_than_ten_mins_late"),
          row.getAs[Double]("p_more_than_ten_mins_late")
        )
      } should contain theSameElementsAs expectedResult
    }
  }
}

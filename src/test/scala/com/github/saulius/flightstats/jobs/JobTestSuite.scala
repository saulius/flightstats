package com.github.saulius.flightstats.jobs

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.scalatest.{FunSpec, Matchers, BeforeAndAfterAll}

class JobTestSuite extends FunSpec with Matchers with BeforeAndAfterAll {
  var _sparkContext: SparkContext = null
  var _sqlContext: SQLContext = null
  lazy val sparkContext = _sparkContext
  lazy val sqlContext = _sqlContext

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("JobTestSuite")

    _sqlContext = new SQLContext(new SparkContext(conf))
    _sparkContext = sqlContext.sparkContext
  }

  override protected def afterAll(): Unit = {
    try {
      _sqlContext.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  import sqlContext.implicits._

  describe("TopAirportsJobTest.process") {
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

  describe("TopDelayedLinksJobTest.process") {
    it("returns top delayed links by arrival delay time") {
      val testDF = Seq(
        ("ORD", "ATL", 10, 2016, 1, 1),
        ("ORD", "ATL", 80, 2016, 4, 5),
        ("ATL", "DFW", 20, 2015, 2, 3),
        ("ATL", "ORD", -10, 2013,8, 9)  // filtered out, negative arrival delay
      ).toDF("Origin", "Dest", "ArrDelay", "Year", "Month", "DayofMonth")

      val job = new TopDelayedLinksJob(sqlContext)

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
}

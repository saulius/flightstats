package com.github.saulius.flightstats.jobs

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

import com.github.saulius.flightstats.Job

class TopDelayedLinksJob(sqlContext: SQLContext) extends Job(sqlContext) {
  import sqlContext.implicits._

  val showRows = 20

  def process(input: DataFrame) =
    input
      .filter($"ArrDelay" > lit(0))
      .select(
        $"ArrDelay",
        concat($"Origin", lit(" -> "), $"Dest") as "link",
        concat($"Year",
               lit("-"),
               lpad($"Month", 2, "0"),
               lit("-"),
               lpad($"DayofMonth", 2, "0")) as "date")
      .groupBy($"link")
      .agg(sum($"ArrDelay") as "combined_delay",
           min($"date") as "earliest_date",
           max($"date") as "latest_date",
           count($"link") as "number_of_links")
      .orderBy($"combined_delay".desc)
}

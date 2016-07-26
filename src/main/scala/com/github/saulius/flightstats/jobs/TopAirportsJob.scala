package com.github.saulius.flightstats.jobs

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{count, array}

import com.github.saulius.flightstats.Job

class TopAirportsJob(sqlContext: SQLContext) extends Job(sqlContext) {
  import sqlContext.implicits._

  def process(input: DataFrame) =
    input
      .select(array($"Origin", $"Dest") as "airports")
      .explode("airports", "airport") { airports: Seq[String] => airports }
      .select($"airport")
      .groupBy($"airport")
      .agg(count("*") as "flight_count")
      .orderBy($"flight_count".desc)
      .limit(20)
}

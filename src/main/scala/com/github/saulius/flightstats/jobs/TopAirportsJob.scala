package com.github.saulius.flightstats.jobs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{count, array, explode}

import com.github.saulius.flightstats.Job

class TopAirportsJob(sparkSession: SparkSession) extends Job(sparkSession) {
  import sparkSession.implicits._

  val showRows = 20

  def process(input: DataFrame) =
    input
      .select(explode(array($"Origin", $"Dest")) as "airport")
      .groupBy($"airport")
      .agg(count("*") as "flight_count")
      .orderBy($"flight_count".desc)
}

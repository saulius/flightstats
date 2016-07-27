package com.github.saulius.flightstats.jobs

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

import com.github.saulius.flightstats.Job

class ArrivalDelayPredictionJob(sqlContext: SQLContext) extends Job(sqlContext) {
  import sqlContext.implicits._

  val HOURS_PER_DEPARTURE_TIME_BLOCK = 4

  def process(input: DataFrame) =
    input
      .select(
        concat($"Origin", lit(" -> "), $"Dest") as "route",
        $"DayOfWeek",
        floor($"DepTime" / 100 / HOURS_PER_DEPARTURE_TIME_BLOCK).as("departure_time_block"),
        when($"ArrDelay" === lit(0), 1).otherwise(0) as "on_time",
        when($"ArrDelay" > lit(0) && $"ArrDelay" <= lit(10), 1).otherwise(0) as "less_than_ten_mins_late",
        when($"ArrDelay" > lit(10), 1).otherwise(0) as "more_than_ten_mins_late")
      .groupBy($"route", $"DayOfWeek", $"departure_time_block")
      .agg(
        count("*") as "total",
        sum($"on_time") as "on_time",
        sum($"less_than_ten_mins_late") as "less_than_ten_mins_late",
        sum($"more_than_ten_mins_late") as "more_than_ten_mins_late")
      .select(
        $"route",
        $"DayOfWeek" as "day_of_week",
        $"departure_time_block",
        round($"on_time".cast("float") / $"total" * 100) as "p_on_time",
        round($"less_than_ten_mins_late".cast("float") / $"total" * 100) as "p_less_than_ten_mins_late",
        round($"more_than_ten_mins_late".cast("float") / $"total" * 100) as "p_more_than_ten_mins_late")
      .orderBy($"p_more_than_ten_mins_late".desc)
      .limit(100)
}

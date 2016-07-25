package com.github.saulius.flightstats.jobs

import org.apache.spark.sql.{DataFrame, SQLContext}

import com.github.saulius.flightstats.Job

class TopAirportsJob(sqlContext: SQLContext) extends Job(sqlContext) {
  def process(input: DataFrame) = input
  def run(inputPath: String) = println(s"hello world: ${inputPath}")
}

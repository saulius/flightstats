package com.github.saulius.flightstats

import org.apache.spark.sql.{DataFrame, SQLContext}

trait RunnableJob {
  def run(inputPath: String): Unit
}

abstract class Job(sqlContext: SQLContext) extends RunnableJob {
  def run(inputPath: String): Unit
  def process(input: DataFrame): DataFrame
}

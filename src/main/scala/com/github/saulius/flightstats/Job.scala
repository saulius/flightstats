package com.github.saulius.flightstats

import org.apache.spark.sql.{DataFrame, SparkSession}

trait RunnableJob {
  def run(inputPath: String): Unit
}

abstract class Job(sparkSession: SparkSession) extends RunnableJob {
  // How many rows to include in the result
  def showRows: Int

  def run(inputPath: String) =
    process(
      sparkSession
        .read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(inputPath)
    ).limit(showRows).show(numRows = showRows)

  def process(input: DataFrame): DataFrame
}

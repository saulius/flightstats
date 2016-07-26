package com.github.saulius.flightstats

import org.apache.spark.sql.{DataFrame, SQLContext}

trait RunnableJob {
  def run(inputPath: String): Unit
}

abstract class Job(sqlContext: SQLContext) extends RunnableJob {
  def run(inputPath: String) =
    process(
      sqlContext
        .read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(inputPath)
    ).show

  def process(input: DataFrame): DataFrame
}

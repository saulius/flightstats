package com.github.saulius.flightstats

import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object JobRunner {
  def main(args: Array[String]): Unit = {
    require(args.length == 2, "Usage: com.github.saulius.flightstats.JobRunner <JobClassName> <jobInputFolder>")
    val Array(jobClassName, inputPath) = args

    val sparkContext = new SparkContext(new SparkConf().setAppName(jobClassName))
    val sqlContext = new SQLContext(sparkContext)

    getJobInstance(jobClassName, sqlContext) match {
      case Some(jobInstance) => jobInstance.run(inputPath)
      case None => println(s"Could not find job class: ${jobClassName}")
    }
  }

  private def getJobInstance(className: String, sqlContext: SQLContext): Option[RunnableJob] = {
    try {
      Some(
        Class.forName(className)
          .getConstructors()(0)
          .newInstance(sqlContext)
          .asInstanceOf[RunnableJob])
    } catch {
      case NonFatal(e) => {
        None
      }
    }
  }
}

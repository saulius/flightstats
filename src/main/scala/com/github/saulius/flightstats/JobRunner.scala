package com.github.saulius.flightstats

import scala.util.control.NonFatal

import org.apache.spark.sql.SparkSession

object JobRunner {
  def main(args: Array[String]): Unit = {
    require(args.length == 2, "Usage: com.github.saulius.flightstats.JobRunner <JobClassName> <jobInputFolder>")
    val Array(jobClassName, inputPath) = args

    val sparkSession = SparkSession.builder.getOrCreate()

    getJobInstance(jobClassName, sparkSession) match {
      case Some(jobInstance) => jobInstance.run(inputPath)
      case None => println(s"Could not find job class: ${jobClassName}")
    }
  }

  private def getJobInstance(className: String, sparkSession: SparkSession): Option[RunnableJob] = {
    try {
      Some(
        Class.forName(className)
          .getConstructors()(0)
          .newInstance(sparkSession)
          .asInstanceOf[RunnableJob])
    } catch {
      case NonFatal(e) => {
        None
      }
    }
  }
}

# flightstats

## Building the project

1. Get JDK. Tested with `1.7.0_55` and `1.8.0_91` on OSX
2. Execute `./gradlew clean shadowJar` (or `./gradlew.bat clean shadowJar` on Windows)

## Running unit tests

Execute `./gradlew test` (or `./gradlew.bat test` on Windows)

## Running locally

1. Download `csv` files from [http://stat-computing.org/dataexpo/2009/the-data.html](http://stat-computing.org/dataexpo/2009/the-data.html) and uncompress them.
2. Download Spark distribution from [http://spark.apache.org/downloads.html](http://spark.apache.org/downloads.html). Tested with `spark-2.0.0-bin-hadoop2.7.tgz` only.
3. Build the project.
4. Execute: `spark-2.0.0-bin-hadoop2.7/bin/spark-submit --master "local[*]" --class com.github.saulius.flightstats.JobRunner build/libs/flightstats-all.jar com.github.saulius.flightstats.jobs.ArrivalDelayPredictionJob data`
  Assuming here that Spark was downloaded to the project directory and the data resides in `data` directory on project root.

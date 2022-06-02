package org.example

import org.apache.spark.sql.SparkSession

object Counting {
  val logFile = "/Users/cubean/spark/README.md" // Should be some file on your system

  def getCount(spark: SparkSession): Long = {
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")

    numAs
  }
}

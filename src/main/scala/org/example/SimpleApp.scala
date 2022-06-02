/* SimpleApp.scala */
package org.example
import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    Counting.getCount(spark)
    spark.stop()
  }
}
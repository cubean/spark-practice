package org.example

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object JsonFlatten {

  def flattenDF(df: DataFrame): DataFrame = {
    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    for (i <- fields.indices) {
      val field = fields(i)
      val fieldType = field.dataType
      val fieldName = field.name
      fieldType match {
        case _: ArrayType =>
          val firstFieldName = fieldName
          val fieldNamesExcludingArrayType = fieldNames.filter(_ != firstFieldName)
          val explodeFieldNames = fieldNamesExcludingArrayType ++ Array(s"explode_outer($firstFieldName) as $firstFieldName")
          val explodedDf = df.selectExpr(explodeFieldNames: _*)
          return flattenDF(explodedDf)

        case sType: StructType =>
          val childFieldNames = sType.fieldNames.map(childName => fieldName + "." + childName)
          val newFieldNames = fieldNames.filter(_ != fieldName) ++ childFieldNames
          val renamedCols = newFieldNames.map(x => col(x.toString()).as(x.toString().replace(
            ".", "_").replace(
            "$", "_").replace(
            "__", "_").replace(
            " ", "").replace(
            "-", "")))
          val explodeDf = df.select(renamedCols: _*)
          return flattenDF(explodeDf)
        case _ =>
      }
    }
    df
  }

  def getCount(spark: SparkSession): DataFrame = {
    val df = spark.read.option("multiline","true").json("data/example1.json").cache()
    val dfFlatten = flattenDF(df)
    dfFlatten.show(false)
    println(s"Lines: ${dfFlatten.count()}")
    dfFlatten
  }
}

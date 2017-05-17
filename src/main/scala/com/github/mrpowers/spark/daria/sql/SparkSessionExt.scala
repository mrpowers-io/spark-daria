package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StructField, StructType}

object SparkSessionExt {

  implicit class SparkSessionMethods(spark: SparkSession) {

    // double definition error due to Type Erasure: http://stackoverflow.com/a/4982668/1125159

    private def asRows[U](values: List[U]): List[Row] = {
      values map {
        case x: Row => x.asInstanceOf[Row]
        case y: Product => Row(y.productIterator.toList: _*)
        case a => Row(a)
      }
    }

    def createDF[U](
      rowData: List[U],
      fields: List[(String, DataType, Boolean)]
    ): DataFrame = {
      val structFields = fields.map(field => {
        StructField(field._1, field._2, field._3)
      })
      spark.createDF(asRows(rowData), structFields)
    }

    // weird method signature per this Stackoverflow thread: http://stackoverflow.com/a/4982668/1125159

    def createDF[U](
      rowData: List[U],
      schema: List[StructField]
    )(implicit i1: DummyImplicit): DataFrame = {
      spark.createDataFrame(
        spark.sparkContext.parallelize(asRows(rowData)),
        StructType(schema)
      )
    }
  }
}

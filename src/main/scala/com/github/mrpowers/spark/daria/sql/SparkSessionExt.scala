package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StructField, StructType}

object SparkSessionExt {

  implicit class SparkSessionMethods(spark: SparkSession) {

    def createDF(rowData: List[Row], schema: List[StructField]): DataFrame = {
      spark.createDataFrame(
        spark.sparkContext.parallelize(rowData),
        StructType(schema)
      )
    }

    // weird method signature per this Stackoverflow thread: http://stackoverflow.com/a/4982668/1125159
    def createDF(
      rowData: List[Row],
      fields: List[(String, DataType, Boolean)]
    )(implicit i1: DummyImplicit): DataFrame = {
      val structFields = fields.map(field => {
        StructField(field._1, field._2, field._3)
      })
      spark.createDataFrame(
        spark.sparkContext.parallelize(rowData),
        StructType(structFields)
      )
    }

    implicit def value2tuple[T](x: T): Tuple1[T] = Tuple1(x)

    def createDF(
      values: List[Product],
      fields: List[(String, DataType, Boolean)]
    )(implicit i1: DummyImplicit, i2: DummyImplicit): DataFrame = {
      val rows = values.map(value => {
        Row(value.productIterator.toList: _*)
      })
      val structFields = fields.map(field => {
        StructField(field._1, field._2, field._3)
      })
      spark.createDF(rows, structFields)
    }

  }

}


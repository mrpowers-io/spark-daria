package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}

object SparkSessionExt {

  implicit class SparkSessionMethods(spark: SparkSession) {

    def createDF(rowData: List[Row], schema: List[StructField]): DataFrame = {
      spark.createDataFrame(
        spark.sparkContext.parallelize(rowData),
        StructType(schema)
      )
    }

  }

}


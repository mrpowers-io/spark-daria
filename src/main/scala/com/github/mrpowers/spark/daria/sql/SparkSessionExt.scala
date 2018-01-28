package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StructField, StructType}

object SparkSessionExt {

  implicit class SparkSessionMethods(spark: SparkSession) {

    private def asRows[U](values: List[U]): List[Row] = {
      values map {
        case x: Row => x.asInstanceOf[Row]
        case y: Product => Row(y.productIterator.toList: _*)
        case a => Row(a)
      }
    }

    private def asSchema[U](fields: List[U]): List[StructField] = {
      fields map {
        case x: StructField => x.asInstanceOf[StructField]
        case (name: String, dataType: DataType, nullable: Boolean) =>
          StructField(name, dataType, nullable)
      }
    }

    /**
      * Creates a DataFrame, similar to createDataFrame, but with better syntax
      * spark-daria defined a createDF method that allows for the terse syntax of `toDF` and the control of `createDataFrame`.
      *
      * spark.createDF(
      *   List(
      *     ("bob", 45),
      *     ("liz", 25),
      *     ("freeman", 32)
      *   ), List(
      *     ("name", StringType, true),
      *     ("age", IntegerType, false)
      *   )
      * )
      *
      * The `createDF` method can also be used with lists of `Row` and `StructField` objects.
      *
      * spark.createDF(
      *   List(
      *     Row("bob", 45),
      *     Row("liz", 25),
      *     Row("freeman", 32)
      *   ), List(
      *     StructField("name", StringType, true),
      *     StructField("age", IntegerType, false)
      *   )
      * )
      */
    def createDF[U, T](rowData: List[U], fields: List[T]): DataFrame = {
      spark.createDataFrame(
        spark.sparkContext.parallelize(asRows(rowData)),
        StructType(asSchema(fields))
      )
    }

  }

}

package com.github.mrpowers.spark.daria.elt

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types._

class StagingParser(extractSchema: StructType) extends Parser {

  final val validConditionExpr: Column    = col("ts").isNotNull && col("zip").isNotNull
  final val additionalDetailsExpr: Column = concat_ws(":", lit("Input File Name"), input_file_name)

  def parse(source: DataFrame): DataFrame = {
    source
      .withColumn(
        "extractedFields",
        from_json(col("origin.value"), extractSchema)
      )
      .select(
        "extractedFields.*",
        "origin"
      )
  }

}

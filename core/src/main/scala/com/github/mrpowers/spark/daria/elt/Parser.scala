package com.github.mrpowers.spark.daria.elt

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

abstract class Parser {

  val validConditionExpr: Column
  val additionalDetailsExpr: Column

  def prepare(source: DataFrame): DataFrame = source
  def parse(source: DataFrame): DataFrame
  def complete(source: DataFrame): DataFrame = source

  final def apply(source: DataFrame): DataFrame = {
    source
      .transform(prepare)
      .select(struct("*") as 'origin)
      .transform(parse)
      .transform(setParseDetails)
      .transform(complete)
  }

  final def setParseDetails(parsed: DataFrame): DataFrame = {
    parsed.withColumn(
      "parse_details",
      struct(
        when(validConditionExpr, lit("OK")).otherwise(lit("NOT_VALID")) as 'status,
        additionalDetailsExpr as 'info,
        current_timestamp() as 'at
      )
    )
  }

}

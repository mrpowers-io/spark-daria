package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ExampleTransforms {

  def withGreeting()(df: DataFrame): DataFrame = {
    df.withColumn(
      "greeting",
      lit("hello world")
    )
  }

  def withCat(name: String)(df: DataFrame): DataFrame = {
    df.withColumn(
      "cats",
      lit(s"$name meow")
    )
  }

  def dropWordCol()(df: DataFrame): DataFrame = {
    df.drop("word")
  }

}

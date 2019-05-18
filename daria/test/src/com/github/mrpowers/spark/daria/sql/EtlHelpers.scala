package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

object EtlHelpers {

  def someTransform()(df: DataFrame): DataFrame = {
    df.withColumn(
      "cool",
      lit("dude")
    )
  }

  def someWriter()(df: DataFrame): Unit = {
    val path = new java.io.File("./tmp/example").getCanonicalPath
    df.repartition(1).write.mode(SaveMode.Overwrite).csv(path)
  }

}

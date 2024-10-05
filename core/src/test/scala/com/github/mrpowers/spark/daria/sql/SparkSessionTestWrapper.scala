package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    val session = SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config(
        "spark.sql.shuffle.partitions",
        "1"
      )
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    session
  }

}

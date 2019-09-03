package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}

class ParquetCompactor(dirname: String, numOutputFiles: Int) {

  def run(): Unit = {

    lazy val spark: SparkSession = {
      SparkSession
        .builder()
        .master("local")
        .appName("spark session")
        .getOrCreate()
    }

    val df = spark.read.parquet(dirname)

    df.withColumn("input_file_name_part", regexp_extract(input_file_name(), """part.+c\d{3}""", 0))
      .select("input_file_name_part")
      .distinct
      .write
      .parquet(s"$dirname/input_file_name_parts")

    val fileNames = spark.read
      .parquet(s"$dirname/input_file_name_parts")
      .collect
      .map((r: Row) => r(0).asInstanceOf[String])

    val uncompactedDF = spark.read
      .parquet(s"$dirname/{${fileNames.mkString(",")}}*.parquet")

    uncompactedDF
      .coalesce(numOutputFiles)
      .write
      .mode(SaveMode.Append)
      .parquet(dirname)

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    fileNames.foreach { filename: String =>
      fs.delete(new Path(s"$dirname/$filename.snappy.parquet"), false)
    }

    fs.delete(new Path(s"$dirname/input_file_name_parts"), true)

  }

}

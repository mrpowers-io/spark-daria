package com.github.mrpowers.spark.daria.utils

import org.apache.spark.sql.SparkSession

object DirHelpers {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .getOrCreate()
  }

  def numBytes(dirname: String): Long = {
    val filePath   = new org.apache.hadoop.fs.Path(dirname)
    val fileSystem = filePath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fileSystem.getContentSummary(filePath).getLength
  }

  def bytesToGb(bytes: Long): Long = {
    bytes / 1073741824L
  }

  def num1GBPartitions(gigabytes: Long): Int = {
    if (gigabytes == 0L) 1 else gigabytes.toInt
  }

}

package com.github.mrpowers.spark.daria.delta

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DeltaLogHelpers {

  // get the deltaLogDF with this code
  // DeltaLog
  //   .forTable(spark, path)
  //   .snapshot
  //   .allFiles
  def num1GbPartitions(deltaLogDF: DataFrame, minNumPartitions: Int = 1): Int = {

    val numBytes = deltaLogDF
      .agg(sum("size"))
      .head
      .getLong(0)
    val numGigabytes = numBytes / 1073741824L
    if (numGigabytes < minNumPartitions) minNumPartitions else numGigabytes.toInt

  }

  def withBytesToGigabytes(bytesColName: String, outputColName: String = "gigabytes")(df: DataFrame): DataFrame = {
    df.withColumn(outputColName, col(bytesColName) / lit(1073741824L))
  }

  def partitionedLake1GbChunks(deltaLogDF: DataFrame, partitionKey: String, minNumPartitions: Int = 1): DataFrame = {

    deltaLogDF
      .withColumn("partitionValue", col("partitionValues")(partitionKey))
      .groupBy("partitionValue")
      .sum()
      .transform(withBytesToGigabytes("sum(size)"))
      .withColumn(
        "num_1GB_partitions",
        when(
          col("gigabytes") < minNumPartitions,
          minNumPartitions
        ).otherwise(col("gigabytes").cast("int"))
      )

  }

}

package com.github.mrpowers.spark.daria.delta

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DeltaLogHelpers {

  def num1GbPartitions(deltaLogDF: DataFrame, minNumPartitions: Int = 1): Int = {

    val numBytes = deltaLogDF
      .agg(sum("size"))
      .head
      .getLong(0)
    val numGigabytes = numBytes / 1073741824L
    if (numGigabytes < minNumPartitions) minNumPartitions else numGigabytes.toInt

  }

}

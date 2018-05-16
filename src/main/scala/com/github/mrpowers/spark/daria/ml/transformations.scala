package com.github.mrpowers.spark.daria.ml

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql._

object transformations {

  /**
   * Converts all the features into a vector
   * All of the feature columns should be DoubleType
   */
  def withVectorizedFeatures(
    featureColNames: Array[String],
    outputColName: String = "features"
  )(df: DataFrame): DataFrame = {
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(featureColNames)
      .setOutputCol(outputColName)
    assembler.transform(df)
  }

}

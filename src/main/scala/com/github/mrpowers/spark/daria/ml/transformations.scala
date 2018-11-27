package com.github.mrpowers.spark.daria.ml

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql._

object transformations {

  /**
   * Converts all the features into a vector
   * All of the feature columns should be DoubleType
   */
  def withVectorizedFeatures(
    featureColNames: Array[String],
    outputColName: String = "features"
  )(df: DataFrame
  ): DataFrame = {
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(featureColNames)
      .setOutputCol(outputColName)
    assembler.transform(df)
  }

  def withLabel(inputColName: String, outputColName: String = "label")(df: DataFrame) = {
    val labelIndexer: StringIndexer = new StringIndexer()
      .setInputCol(inputColName)
      .setOutputCol(outputColName)

    labelIndexer
      .fit(df)
      .transform(df)
  }

}

package com.github.mrpowers.spark.daria.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

object DariaValidator {

  /**
   * Throws an error if the DataFrame doesn't contain all the required columns
   * Validates if columns are included in a DataFrame. This code will error out:
   *
   * {{{
   * val sourceDF = Seq(
   *   ("jets", "football"),
   *   ("nacional", "soccer")
   * ).toDF("team", "sport")
   *
   * val requiredColNames = Seq("team", "sport", "country", "city")
   *
   * validatePresenceOfColumns(sourceDF, requiredColNames)
   * }}}
   *
   * This is the error message
   *
   * > com.github.mrpowers.spark.daria.sql.MissingDataFrameColumnsException: The [country, city] columns are not included in the DataFrame with the following columns [team, sport]
   */
  def validatePresenceOfColumns(df: DataFrame, requiredColNames: Seq[String]): Unit = {
    val c = new DataFrameColumnsChecker(
      df,
      requiredColNames
    )
    c.validatePresenceOfColumns()
  }

  /**
   * Throws an error if the DataFrame schema doesn't match the required schema
   *
   * This code will error out:
   *
   * {{{
   * val sourceData = List(
   *   Row(1, 1),
   *  Row(-8, 8),
   *  Row(-5, 5),
   *  Row(null, null)
   * )
   *
   * val sourceSchema = List(
   *   StructField("num1", IntegerType, true),
   *   StructField("num2", IntegerType, true)
   * )
   *
   * val sourceDF = spark.createDataFrame(
   *   spark.sparkContext.parallelize(sourceData),
   *   StructType(sourceSchema)
   * )
   *
   * val requiredSchema = StructType(
   *   List(
   *     StructField("num1", IntegerType, true),
   *     StructField("num2", IntegerType, true),
   *     StructField("name", StringType, true)
   *   )
   * )
   *
   * validateSchema(sourceDF, requiredSchema)
   * }}}
   *
   * This is the error message:
   *
   * > com.github.mrpowers.spark.daria.sql.InvalidDataFrameSchemaException: The [StructField(name,StringType,true)] StructFields are not included in the DataFrame with the following StructFields [StructType(StructField(num1,IntegerType,true), StructField(num2,IntegerType,true))]
   */
  def validateSchema(df: DataFrame, requiredSchema: StructType): Unit = {
    val c = new DataFrameSchemaChecker(
      df,
      requiredSchema
    )
    c.validateSchema()
  }

  /**
   * Throws an error if the DataFrame contains any of the prohibited columns
   * Validates columns are not included in a DataFrame. This code will error out:
   *
   * {{{
   * val sourceDF = Seq(
   *   ("jets", "football"),
   *   ("nacional", "soccer")
   * ).toDF("team", "sport")
   *
   * val prohibitedColNames = Seq("team", "sport", "country", "city")
   *
   * validateAbsenceOfColumns(sourceDF, prohibitedColNames)
   * }}}
   *
   * This is the error message:
   *
   * > com.github.mrpowers.spark.daria.sql.ProhibitedDataFrameColumnsException: The [team, sport] columns are not allowed to be included in the DataFrame with the following columns [team, sport]
   */
  def validateAbsenceOfColumns(df: DataFrame, prohibitedColNames: Seq[String]): Unit = {
    val c = new DataFrameColumnsAbsence(
      df,
      prohibitedColNames
    )
    c.validateAbsenceOfColumns()
  }

}

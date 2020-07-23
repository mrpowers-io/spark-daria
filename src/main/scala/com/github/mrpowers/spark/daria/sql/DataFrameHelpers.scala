package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

object DataFrameHelpers extends DataFrameValidator {

  /**
   * Converts two column to a map of key value pairs
   *
   * '''N.B. This method uses `collect` and should only be called on small DataFrames.'''
   *
   * Converts two columns in a DataFrame to a Map.
   *
   * Suppose we have the following `sourceDF`:
   *
   * {{{
   * +-----------+---------+
   * |     island|fun_level|
   * +-----------+---------+
   * |    boracay|        7|
   * |long island|        9|
   * +-----------+---------+
   * }}}
   *
   * Let's convert this DataFrame to a Map with `island` as the key and `fun_level` as the value.
   *
   * {{{
   * val actual = DataFrameHelpers.twoColumnsToMap[String, Integer](
   *   sourceDF,
   *   "island",
   *   "fun_level"
   * )
   *
   * println(actual)
   *
   * // Map(
   * //   "boracay" -> 7,
   * //   "long island" -> 9
   * // )
   * }}}
   */
  def twoColumnsToMap[keyType: TypeTag, valueType: TypeTag](df: DataFrame, keyColName: String, valueColName: String): Map[keyType, valueType] = {
    validatePresenceOfColumns(
      df,
      Seq(
        keyColName,
        valueColName
      )
    )
    df.select(
      keyColName,
      valueColName
    ).collect()
      .map(r => (r(0).asInstanceOf[keyType], r(1).asInstanceOf[valueType]))
      .toMap
  }

  /**
   * Converts a DataFrame column to an Array of values
   * '''N.B. This method uses `collect` and should only be called on small DataFrames.'''
   *
   * This function converts a column to an array of items.
   *
   * Suppose we have the following `sourceDF`:
   *
   * {{{
   * +---+
   * |num|
   * +---+
   * |  1|
   * |  2|
   * |  3|
   * +---+
   * }}}
   *
   * Let's convert the `num` column to an Array of values.  Let's run the code and view the results.
   *
   * {{{
   * val actual = DataFrameHelpers.columnToArray[Int](sourceDF, "num")
   *
   * println(actual)
   *
   * // Array(1, 2, 3)
   * }}}
   */
  def columnToArray[T: ClassTag](df: DataFrame, colName: String): Array[T] = {
    df.select(colName).collect().map(r => r(0).asInstanceOf[T])
  }

  /**
   * Converts a DataFrame column to a List of values
   * '''N.B. This method uses `collect` and should only be called on small DataFrames.'''
   *
   * This function converts a column to a list of items.
   *
   * Suppose we have the following `sourceDF`:
   *
   * {{{
   * +---+
   * |num|
   * +---+
   * |  1|
   * |  2|
   * |  3|
   * +---+
   * }}}
   *
   * Let's convert the `num` column to a List of values.  Let's run the code and view the results.
   *
   * {{{
   * val actual = DataFrameHelpers.columnToList[Int](sourceDF, "num")
   *
   * println(actual)
   *
   * // List(1, 2, 3)
   * }}}
   */
  def columnToList[T: ClassTag](df: DataFrame, colName: String): List[T] = {
    columnToArray[T](
      df,
      colName
    ).toList
  }

  /**
   * Converts a DataFrame to an Array of Maps
   * '''N.B. This method uses `collect` and should only be called on small DataFrames.'''
   *
   * Converts a DataFrame to an array of Maps.
   *
   * Suppose we have the following `sourceDF`:
   *
   * {{{
   * +----------+-----------+---------+
   * |profession|some_number|pay_grade|
   * +----------+-----------+---------+
   * |    doctor|          4|     high|
   * |   dentist|         10|     high|
   * +----------+-----------+---------+
   * }}}
   *
   * Run the code to convert this DataFrame into an array of Maps.
   *
   * {{{
   * val actual = DataFrameHelpers.toArrayOfMaps(sourceDF)
   *
   * println(actual)
   *
   * Array(
   *   Map("profession" -> "doctor", "some_number" -> 4, "pay_grade" -> "high"),
   *   Map("profession" -> "dentist", "some_number" -> 10, "pay_grade" -> "high")
   * )
   * }}}
   */
  def toArrayOfMaps(df: DataFrame) = {
    df.collect.map(r => Map(df.columns.zip(r.toSeq): _*))
  }

  /**
   * Generates a CREATE TABLE query for AWS Athena
   *
   * Suppose we have the following `df`:
   *
   * {{{
   * +--------+--------+---------+
   * |    team|   sport|goals_for|
   * +--------+--------+---------+
   * |    jets|football|       45|
   * |nacional|  soccer|       10|
   * +--------+--------+---------+
   * }}}
   *
   * Run the code to print the CREATE TABLE query.
   *
   * {{{
   * DataFrameHelpers.printAthenaCreateTable(df, "my_cool_athena_table", "s3://my-bucket/extracts/people")
   *
   * CREATE TABLE IF NOT EXISTS my_cool_athena_table(
   *   team STRING,
   *   sport STRING,
   *   goals_for INT
   * )
   * STORED AS PARQUET
   * LOCATION 's3://my-bucket/extracts/people'
   * }}}
   */
  def printAthenaCreateTable(df: DataFrame, athenaTableName: String, s3location: String): Unit = {
    val fields = df.schema.map { (f: StructField) =>
      s"${f.name} ${sparkTypeToAthenaType(f.dataType.toString)}"
    }

    println(s"CREATE EXTERNAL TABLE IF NOT EXISTS $athenaTableName(")
    println("  " + fields.mkString(",\n  "))
    println(")")
    println("STORED AS PARQUET")
    println(s"LOCATION '$s3location'")
  }

  private[sql] def sparkTypeToAthenaType(sparkType: String): String = {
    sparkType match {
      case "StringType"    => "STRING"
      case "IntegerType"   => "INT"
      case "DateType"      => "DATE"
      case "DecimalType"   => "DECIMAL"
      case "FloatType"     => "FLOAT"
      case "LongType"      => "BIGINT"
      case "TimestampType" => "TIMESTAMP"
      case _               => "STRING"
    }

  }

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .getOrCreate()
  }

  def writeTimestamped(df: DataFrame, outputDirname: String, numPartitions: Option[Int] = None, overwriteLatest: Boolean = true): Unit = {
    val timestamp: String  = new java.text.SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date())
    val outputPath: String = outputDirname + "/" + timestamp
    if (numPartitions.isEmpty) {
      df.write.parquet(outputPath)
    } else {
      val p = numPartitions.get
      df.repartition(p).write.parquet(outputPath)
    }

    if (overwriteLatest) {
      val latestData = Seq(
        Row(
          outputPath
        )
      )

      val latestSchema = List(
        StructField(
          "latest_path",
          StringType,
          false
        )
      )

      val latestDF = spark.createDataFrame(
        spark.sparkContext.parallelize(latestData),
        StructType(latestSchema)
      )

      latestDF.write
        .option(
          "header",
          "false"
        )
        .option(
          "delimiter",
          ","
        )
        .mode(SaveMode.Overwrite)
        .csv(outputDirname + "/latest")
    }
  }

  def readTimestamped(dirname: String): DataFrame = {
    val latestDF = spark.read
      .option(
        "header",
        "false"
      )
      .option(
        "delimiter",
        ","
      )
      .csv(dirname + "/latest")

    val latestPath = latestDF.head().getString(0)

    spark.read.parquet(latestPath)
  }

}

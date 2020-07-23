package com.github.mrpowers.spark.daria.sql

import com.github.mrpowers.spark.daria.sql.functions.truncate
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}

case class InvalidColumnSortOrderException(smth: String) extends Exception(smth)

/**
 * Functions available for DataFrame operations.
 *
 * SQL transformations take a DataFrame as an argument and return a DataFrame.  They are suitable arguments for the `Dataset#transform` method.
 *
 * It's convenient to work with DataFrames that have snake_case column names.  Column names with spaces make it harder to write SQL queries.
 */
object transformations {

  /**
   * Sorts the columns of a DataFrame alphabetically
   * The `sortColumns` transformation sorts the columns in a DataFrame alphabetically.
   *
   * Suppose you start with the following `sourceDF`:
   *
   * {{{
   * +-----+---+-----+
   * | name|age|sport|
   * +-----+---+-----+
   * |pablo|  3| polo|
   * +-----+---+-----+
   * }}}
   *
   * Run the code:
   *
   * {{{
   * val actualDF = sourceDF.transform(sortColumns())
   * }}}
   *
   * Here’s the `actualDF`:
   *
   * {{{
   * +---+-----+-----+
   * |age| name|sport|
   * +---+-----+-----+
   * |  3|pablo| polo|
   * +---+-----+-----+
   * }}}
   */
  def sortColumns(order: String = "asc")(df: DataFrame): DataFrame = {
    val colNames = if (order == "asc") {
      df.columns.sorted
    } else if (order == "desc") {
      df.columns.sorted.reverse
    } else {
      val message =
        s"The sort order must be 'asc' or 'desc'.  Your sort order was '$order'."
      throw new InvalidColumnSortOrderException(message)
    }
    val cols = colNames.map(col(_))
    df.select(cols: _*)
  }

  /**
   * snake_cases all the columns of a DataFrame
   * spark-daria defines a `com.github.mrpowers.spark.daria.sql.transformations.snakeCaseColumns` transformation to convert all the column names to snake\_case.
   *
   * import com.github.mrpowers.spark.daria.sql.transformations._
   *
   * {{{
   * val sourceDf = Seq(
   *   ("funny", "joke")
   * ).toDF("A b C", "de F")
   *
   * val actualDf = sourceDf.transform(snakeCaseColumns)
   *
   * actualDf.show()
   *
   * +-----+----+
   * |a_b_c|de_f|
   * +-----+----+
   * |funny|joke|
   * +-----+----+
   * }}}
   */
  def snakeCaseColumns()(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (memoDF, colName) =>
      memoDF.withColumnRenamed(
        colName,
        com.github.mrpowers.spark.daria.utils.StringHelpers.toSnakeCase(colName)
      )
    }
  }

  /**
   * snakifies all the columns of a DataFrame
   *
   * import com.github.mrpowers.spark.daria.sql.transformations._
   *
   * {{{
   * val sourceDf = Seq(
   *   ("funny", "joke")
   * ).toDF("ThIs", "BiH")
   *
   * val actualDf = sourceDf.transform(snakeCaseColumns)
   *
   * actualDf.show()
   *
   * +-----+----+
   * |th_is|bi_h|
   * +-----+----+
   * |funny|joke|
   * +-----+----+
   * }}}
   */
  def snakifyColumns()(df: DataFrame): DataFrame = {
    modifyColumnNames(com.github.mrpowers.spark.daria.utils.StringHelpers.snakify)(df)
  }

  /**
   * Strips out invalid characters and replaces spaces with underscores
   * to make Parquet compatible column names
   */
  def withParquetCompatibleColumnNames()(df: DataFrame): DataFrame = {
    modifyColumnNames(com.github.mrpowers.spark.daria.utils.StringHelpers.parquetCompatibleColumnName)(df)
  }

  /**
   * Changes all the column names in a DataFrame
   */
  def modifyColumnNames(stringFun: String => String)(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (memoDF, colName) =>
      memoDF.withColumnRenamed(
        colName,
        stringFun(colName)
      )
    }
  }

  /**
   * Convert camel case columns to snake case
   * Example: SomeColumn -> some_column
   */
  def camelCaseToSnakeCaseColumns()(df: DataFrame): DataFrame =
    modifyColumnNames(com.github.mrpowers.spark.daria.utils.StringHelpers.camelCaseToSnakeCase)(df)

  /**
   * Title Cases all the columns of a DataFrame
   */
  def titleCaseColumns()(df: DataFrame): DataFrame = {
    def stringFun(str: String): String = str.toLowerCase().split(' ').map(_.capitalize).mkString(" ")
    modifyColumnNames(stringFun)(df)
  }

  def prependToColName(str: String)(df: DataFrame): DataFrame = {
    def stringFun(s: String): String = str + s
    modifyColumnNames(stringFun)(df)
  }

  /**
   * Runs regexp_replace on multiple columns
   * {{{
   * val actualDF = sourceDF.transform(
   *   transformations.multiRegexpReplace(
   *     List(col("person"), col("phone")),
   *     "cool",
   *     "dude"
   *   )
   * )
   * }}}
   *
   * Replaces all `"cool"` strings in the `person` and `phone` columns with the string `"dude"`.
   */
  def multiRegexpReplace(cols: List[Column], pattern: String = "\u0000", replacement: String = "")(df: DataFrame): DataFrame = {
    cols.foldLeft(df) { (memoDF, col) =>
      memoDF
        .withColumn(
          col.toString(),
          regexp_replace(
            col,
            pattern,
            replacement
          )
        )
    }
  }

  /**
   * Runs regexp_replace on all StringType columns in a DataFrame
   * {{{
   * val actualDF = sourceDF.transform(
   *   transformations.bulkRegexpReplace(
   *     "cool",
   *     "dude"
   *   )
   * )
   * }}}
   *
   * Replaces all `"cool"` strings in all the `sourceDF` columns of `StringType` with the string `"dude"`.
   */
  def bulkRegexpReplace(pattern: String = "\u0000", replacement: String = "")(df: DataFrame): DataFrame = {
    val cols = df.schema
      .filter { (s: StructField) =>
        s.dataType.simpleString == "string"
      }
      .map { (s: StructField) =>
        col(s.name)
      }
      .toList

    multiRegexpReplace(
      cols,
      pattern,
      replacement
    )(df)
  }

  /**
   * Truncates multiple columns in a DataFrame
   * {{{
   * val columnLengths: Map[String, Int] = Map(
   *   "person" -> 2,
   *   "phone" -> 3
   * )
   *
   * sourceDF.transform(
   *   truncateColumns(columnLengths)
   * )
   * }}}
   *
   * Limits the `"person"` column to 2 characters and the `"phone"` column to 3 characters.
   */
  def truncateColumns(columnLengths: Map[String, Int])(df: DataFrame): DataFrame = {
    columnLengths.foldLeft(df) {
      case (memoDF, (colName, length)) =>
        if (memoDF.schema.fieldNames.contains(colName)) {
          memoDF.withColumn(
            colName,
            truncate(
              col(colName),
              length
            )
          )
        } else {
          memoDF
        }
    }
  }

  /**
   * Categorizes a numeric column in various user specified "buckets"
   */
  def withColBucket(
      colName: String,
      outputColName: String,
      buckets: Array[(Any, Any)],
      inclusiveBoundries: Boolean = false,
      lowestBoundLte: Boolean = false,
      highestBoundGte: Boolean = false
  )(df: DataFrame) = {
    df.withColumn(
      outputColName,
      functions.bucketFinder(
        col(colName),
        buckets,
        inclusiveBoundries,
        lowestBoundLte,
        highestBoundGte
      )
    )
  }

  /**
   * Extracts an object from a JSON field with a specified schema
   *
   * {{{
   * val sourceDF = spark.createDF(
   *   List(
   *     (10, """{"name": "Bart cool", "age": 25}"""),
   *     (20, """{"name": "Lisa frost", "age": 27}""")
   *   ), List(
   *     ("id", IntegerType, true),
   *     ("person", StringType, true)
   *   )
   * )
   *
   * val personSchema = StructType(List(
   *   StructField("name", StringType),
   *   StructField("age", IntegerType)
   * ))
   *
   * val actualDF = sourceDF.transform(
   *   transformations.extractFromJson("person", "personData", personSchema)
   * )
   *
   * actualDF.show()
   * +---+---------------------------------+----------------+
   * |id |person                           |personData      |
   * +---+---------------------------------+----------------+
   * |10 |{"name": "Bart cool", "age": 25} |[Bart cool, 25] |
   * |20 |{"name": "Lisa frost", "age": 27}|[Lisa frost, 27]|
   * +---+---------------------------------+----------------+
   * }}}
   */
  def extractFromJson(colName: String, outputColName: String, jsonSchema: StructType)(df: DataFrame): DataFrame = {
    df.withColumn(
      outputColName,
      from_json(
        col(colName),
        jsonSchema
      )
    )
  }

  /**
   * Extracts an object from a JSON field with a specified path expression
   *
   * {{{
   * val sourceDF = spark.createDF(
   *   List(
   *     (10, """{"name": "Bart cool", "age": 25}"""),
   *     (20, """{"name": "Lisa frost", "age": 27}""")
   *   ), List(
   *     ("id", IntegerType, true),
   *     ("person", StringType, true)
   *   )
   * )
   *
   * val actualDF = sourceDF.transform(
   *   transformations.extractFromJson("person", "name", "$.name")
   * )
   *
   * actualDF.show()
   * +---+---------------------------------+----------------+
   * |id |person                           |name            |
   * +---+---------------------------------+----------------+
   * |10 |{"name": "Bart cool", "age": 25} |"Bart cool"     |
   * |20 |{"name": "Lisa frost", "age": 27}|"Lisa frost"    |
   * +---+---------------------------------+----------------+
   * }}}
   */
  def extractFromJson(colName: String, outputColName: String, path: String)(df: DataFrame): DataFrame = {
    df.withColumn(
      outputColName,
      get_json_object(
        col(colName),
        path
      )
    )
  }

  def withRowAsStruct(outputColName: String = "row_as_struct")(df: DataFrame): DataFrame = {
    val colNames = df.columns.map(col)
    df.withColumn(
      outputColName,
      struct(colNames: _*)
    )
  }

}

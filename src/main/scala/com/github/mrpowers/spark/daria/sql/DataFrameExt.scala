package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import com.github.mrpowers.spark.daria.sql.types.StructTypeHelpers

case class DataFrameColumnsException(smth: String) extends Exception(smth)

object DataFrameExt {

  implicit class DataFrameMethods(df: DataFrame) {

    /**
     * Prints the schema with StructType and StructFields so it's easy to copy into code
     * Spark has a `printSchema` method to print the schema of a DataFrame and a `schema` method that returns a `StructType` object.
     *
     * The `Dataset#schema` method can be easily converted into working code for small DataFrames, but it can be a lot of manual work for DataFrames with a lot of columns.
     *
     * The `printSchemaInCodeFormat` DataFrame extension prints the DataFrame schema as a valid `StructType` object.
     *
     * Suppose you have the following `sourceDF`:
     *
     * {{{
     * +--------+--------+---------+
     * |    team|   sport|goals_for|
     * +--------+--------+---------+
     * |    jets|football|       45|
     * |nacional|  soccer|       10|
     * +--------+--------+---------+
     *
     * `sourceDF.printSchemaInCodeFormat()` will output the following rows in the console:
     *
     * StructType(
     *   List(
     *     StructField("team", StringType, true),
     *     StructField("sport", StringType, true),
     *     StructField("goals_for", IntegerType, true)
     *   )
     * )
     * }}}
     */
    def printSchemaInCodeFormat(): Unit = {
      val fields = df.schema.map { (f: StructField) =>
        s"""StructField("${f.name}", ${f.dataType}, ${f.nullable})"""
      }

      println("StructType(")
      println("  List(")
      println("    " + fields.mkString(",\n    "))
      println("  )")
      println(")")
    }

    /**
     * Executes a list of custom DataFrame transformations
     * Uses function composition to run a list of DataFrame transformations.
     *
     * def withGreeting()(df: DataFrame): DataFrame = {
     *   df.withColumn("greeting", lit("hello world"))
     * }
     *
     * def withCat(name: String)(df: DataFrame): DataFrame = {
     *   df.withColumn("cats", lit(name + " meow"))
     * }
     *
     * val transforms = List(
     *   withGreeting()(_),
     *   withCat("sandy")(_)
     * )
     *
     * sourceDF.composeTransforms(transforms)
     */
    def composeTransforms(transforms: List[(DataFrame => DataFrame)]): DataFrame = {
      composeTransforms(transforms: _*)
    }

    /**
     * Executes a list of custom DataFrame transformations
     * Uses function composition to run a list of DataFrame transformations.
     *
     * def withGreeting()(df: DataFrame): DataFrame = {
     *   df.withColumn("greeting", lit("hello world"))
     * }
     *
     * def withCat(name: String)(df: DataFrame): DataFrame = {
     *   df.withColumn("cats", lit(name + " meow"))
     * }
     *
     * sourceDF.composeTransforms(withGreeting(), withCat("sandy"))
     */
    def composeTransforms(transforms: (DataFrame => DataFrame)*): DataFrame = {
      transforms.foldLeft(df) { (memoDF, t) =>
        memoDF.transform(t)
      }
    }

    /**
     * Reorders columns as specified
     * Reorders the columns in a DataFrame.
     *
     * {{{
     * val actualDF = sourceDF.reorderColumns(
     *   Seq("greeting", "team", "cats")
     * )
     * }}}
     *
     * The `actualDF` will have the `greeting` column first, then the `team` column then the `cats` column.
     */
    def reorderColumns(colNames: Seq[String]): DataFrame = {
      val cols = colNames.map(col(_))
      df.select(cols: _*)
    }

    /**
     * Returns true if the DataFrame contains the column
     *
     * {{{
     * sourceDF.containsColumn("team")
     * }}}
     *
     * Returns `true` if `sourceDF` contains a column named `"team"` and false otherwise.
     */
    def containsColumn(colName: String): Boolean = {
      df.schema.fieldNames.contains(colName)
    }

    /** Returns the columns in otherDF that aren't in self */
    def columnDiff(otherDF: DataFrame): Seq[String] = {
      (df.columns).diff(otherDF.columns).toSeq
    }

    /**
     * Like transform(), but for CustomTransform objects
     * Enables you to specify the columns that should be added / removed by a custom transformations and errors out if the columns the columns that are actually added / removed are different.
     *
     * val actualDF = sourceDF
     *   .trans(
     *     CustomTransform(
     *       transform = ExampleTransforms.withGreeting(),
     *       addedColumns = Seq("greeting"),
     *       requiredColumns = Seq("something")
     *     )
     *   )
     *   .trans(
     *     CustomTransform(
     *       transform = ExampleTransforms.withCat("spanky"),
     *       addedColumns = Seq("cats")
     *     )
     *   )
     *   .trans(
     *     CustomTransform(
     *       transform = ExampleTransforms.dropWordCol(),
     *       removedColumns = Seq("word")
     *     )
     *   )
     */
    def trans(customTransform: CustomTransform): DataFrame = {
      // make sure df doesn't already have the columns that will be added
      if (df.columns.toSeq.exists((c: String) => customTransform.addedColumns.contains(c))) {
        throw new DataFrameColumnsException(s"The DataFrame already contains the columns your transformation will add. The DataFrame has these columns: [${df.columns.mkString(", ")}]. You've asserted that your transformation will add these columns: [${customTransform.addedColumns.mkString(", ")}]")
      }

      // make sure df isn't missing the columns that will be dropped
      if (!customTransform.removedColumns.isEmpty && df.columns.toSeq.intersect(customTransform.removedColumns).isEmpty) {
        throw new DataFrameColumnsException(s"The DataFrame does not contain the columns your transformation will drop. The DataFrame has these columns: [${df.columns.mkString(", ")}]. You've asserted that your transformation will drop these columns: [${customTransform.removedColumns.mkString(", ")}]")
      }

      // validate presence of columns
      val c = new DataFrameColumnsChecker(df, customTransform.requiredColumns)
      c.validatePresenceOfColumns()

      val transformedDF = df.transform(customTransform.transform)

      // make sure the columns have been added
      val actualColumnsAdded = transformedDF.columnDiff(df)
      if (!actualColumnsAdded.equals(customTransform.addedColumns)) {
        throw new DataFrameColumnsException(s"The [${actualColumnsAdded.mkString(", ")}] columns were actually added, but you specified that these columns should have been added [${customTransform.addedColumns.mkString(", ")}]")
      }

      // make sure the columns have been removed
      val actualColumnsRemoved = df.columnDiff(transformedDF)
      if (!actualColumnsRemoved.equals(customTransform.removedColumns)) {
        throw new DataFrameColumnsException(s"The [${actualColumnsRemoved.mkString(", ")}] columns were actually removed, but you specified that these columns should have been removed [${customTransform.removedColumns.mkString(", ")}]")
      }

      transformedDF
    }

    def flattenSchema(delimiter: String = ".", prefix: String = null): DataFrame = {
      df.select(
        StructTypeHelpers.flattenSchema(df.schema, delimiter, prefix): _*
      )
    }

    /**
     * Executes a list of transformations in CustomTransform objects
     * Uses function composition
     *
     */
    def composeTrans(customTransforms: CustomTransform*): DataFrame = {
      customTransforms.foldLeft(df) { (memoDF, ct) =>
        memoDF.trans(ct)
      }
    }

  }

}


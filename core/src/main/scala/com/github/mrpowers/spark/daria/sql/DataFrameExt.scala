package com.github.mrpowers.spark.daria.sql

import com.github.mrpowers.spark.daria.sql.types.StructTypeHelpers
import com.github.mrpowers.spark.daria.sql.types.StructTypeHelpers.StructTypeOps
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable

case class DataFrameColumnsException(smth: String) extends Exception(smth)

object DataFrameExt {

  implicit class DataFrameMethods(df: DataFrame) {

    /**
     * Returns a new `DataFrame` with the column `columnName` cast
     * as `newType`.
     *
     * @param columnName the column to cast
     * @param newType the new type for columnName
     */
    def withColumnCast(columnName: String, newType: String): DataFrame =
      df.select((df.columns.map {
        case c if c == columnName => col(c).cast(newType).as(c)
        case c                    => col(c)
      }): _*)

    /**
     * Returns a new `DataFrame` with the column `columnName` cast
     * as `newType`.
     *
     * @param columnName the column to cast
     * @param newType the new type for columnName
     */
    def withColumnCast(columnName: String, newType: DataType): DataFrame =
      df.select((df.columns.map {
        case c if c == columnName => col(c).cast(newType).as(c)
        case c                    => col(c)
      }): _*)

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
      val cols = colNames.map(col)
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

    /**
     * Returns true if the DataFrame contains the StructField
     *
     * {{{
     * sourceDF.containsColumn(StructField("team", StringType, true))
     * }}}
     *
     * Returns `true` if `sourceDF` contains the StructField and false otherwise.
     */
    def containsColumn(structField: StructField): Boolean = {
      df.schema.contains(structField)
    }

    /**
     * Returns true if the DataFrame contains all the columns
     *
     * {{{
     * sourceDF.containsColumns("team", "city")
     * }}}
     *
     * Returns `true` if `sourceDF` contains the `"team"` and `"city"` columns and false otherwise.
     */
    def containsColumns(colNames: String*): Boolean = {
      colNames.forall(df.schema.fieldNames.contains(_))
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
        throw DataFrameColumnsException(
          s"The DataFrame already contains the columns your transformation will add. The DataFrame has these columns: [${df.columns
            .mkString(", ")}]. You've asserted that your transformation will add these columns: [${customTransform.addedColumns
            .mkString(", ")}]"
        )
      }

      // make sure df isn't missing the columns that will be dropped
      if (
        !customTransform.removedColumns.isEmpty && df.columns.toSeq
          .intersect(customTransform.removedColumns)
          .isEmpty
      ) {
        throw new DataFrameColumnsException(
          s"The DataFrame does not contain the columns your transformation will drop. The DataFrame has these columns: [${df.columns
            .mkString(", ")}]. You've asserted that your transformation will drop these columns: [${customTransform.removedColumns
            .mkString(", ")}]"
        )
      }

      // validate presence of columns
      val c = new DataFrameColumnsChecker(
        df,
        customTransform.requiredColumns
      )
      c.validatePresenceOfColumns()

      val transformedDF = df.transform(customTransform.transform)

      // make sure the columns have been added
      val actualColumnsAdded = transformedDF.columnDiff(df)
      if (!actualColumnsAdded.equals(customTransform.addedColumns)) {
        throw DataFrameColumnsException(
          s"The [${actualColumnsAdded.mkString(", ")}] columns were actually added, but you specified that these columns should have been added [${customTransform.addedColumns
            .mkString(", ")}]"
        )
      }

      // make sure the columns have been removed
      val actualColumnsRemoved = df.columnDiff(transformedDF)
      if (!actualColumnsRemoved.equals(customTransform.removedColumns)) {
        throw DataFrameColumnsException(
          s"The [${actualColumnsRemoved.mkString(", ")}] columns were actually removed, but you specified that these columns should have been removed [${customTransform.removedColumns
            .mkString(", ")}]"
        )
      }

      transformedDF
    }

    /**
     * Converts all the StructType columns to regular columns
     * This StackOverflow answer provides a detailed description how to use flattenSchema: https://stackoverflow.com/a/50402697/1125159
     */
    def flattenSchema(delimiter: String = "."): DataFrame = {
      val renamedCols = StructTypeHelpers
        .flattenSchema(df.schema)
        .map(c => c.as(c.toString.replace(".", delimiter)))
      df.select(renamedCols: _*)
    }

    /**
     * This method is opposite of flattenSchema. For example, if you have flat dataframe with snake case columns it will
     * convert it to dataframe with nested columns.
     *
     * From:
     * root
     * |-- person_id: long (nullable = true)
     * |-- person_name: string (nullable = true)
     * |-- person_surname: string (nullable = true)
     *
     * To:
     * root
     * |-- person: struct (nullable = false)
     * |    |-- name: string (nullable = true)
     * |    |-- surname: string (nullable = true)
     * |    |-- id: long (nullable = true)
     */
    def structureSchema(delimiter: String = "_"): DataFrame = {
      def loop(tl: List[(String, List[String])]): List[Column] =
        tl.groupBy { case (_, columnList) => columnList.head }
          .map {
            case (structColumn, l) if l.length > 1 =>
              struct(loop(l.map {
                case (column, _ :: tail) => (column, tail)
                case (column, h :: Nil)  => (column, List(h))
              }): _*).alias(structColumn)
            case (structColumn, l) =>
              l match {
                case (c, h :: Nil) :: Nil => col(c).as(h)
                case _ =>
                  struct(loop(l.map {
                    case (column, _ :: tail) => (column, tail)
                    case (column, h :: Nil)  => (column, List(h))
                  }): _*).alias(structColumn)
              }
          }
          .toList

      df.select(loop(df.columns.toList.map(c => (c, c.split(delimiter).toList))): _*)
    }

    /**
     * Drop nested column by specifying full name (for example foo.bar)
     */
    def dropNestedColumn(fullColumnName: String): DataFrame = {
      val delimiter = "_"
      df.flattenSchema(delimiter)
        .drop(fullColumnName.replace(".", delimiter))
        .structureSchema(delimiter)
    }

    /**
     * Executes a list of transformations in CustomTransform objects
     * Uses function composition
     */
    def composeTrans(customTransforms: List[CustomTransform]): DataFrame = {
      customTransforms.foldLeft(df) { (memoDF, ct) =>
        if (ct.skipWhenPossible && memoDF.containsColumns(ct.addedColumns: _*)) {
          memoDF
        } else {
          memoDF.trans(ct)
        }
      }
    }

    /**
     * Completely removes all duplicates from a DataFrame
     */
    def killDuplicates(cols: Column*): DataFrame = {
      df.withColumn(
        "my_super_secret_count",
        count("*").over(Window.partitionBy(cols: _*))
      ).where(col("my_super_secret_count") === 1)
        .drop(col("my_super_secret_count"))
    }

    /**
     * Completely removes all duplicates from a DataFrame
     */
    def killDuplicates(col1: String, cols: String*): DataFrame = {
      df.killDuplicates((col1 +: cols).map(col): _*)
    }

    /**
     * Completely removes all duplicates from a DataFrame
     */
    def killDuplicates(): DataFrame = {
      df.killDuplicates(df.columns.map(col): _*)
    }

    /**
     * Rename columns
     * Here is how to lowercase all the columns df.renameColumns(_.toLowerCase)
     * Here is how to trim all the columns df.renameColumns(_.trim)
     */
    def renameColumns(f: String => String): DataFrame =
      df.columns.foldLeft(df)((tempDf, c) => tempDf.withColumnRenamed(c, f(c)))

    /**
     * Drops multiple columns that satisfy the conditions of a function
     * Here is how to drop all columns that start with an underscore
     * df.dropColumns(_.startsWith("_"))
     */
    def dropColumns(f: String => Boolean): DataFrame =
      df.columns.foldLeft(df)((tempDf, c) => if (f(c)) tempDf.drop(c) else tempDf)

    /**
     * Makes all columns nullable or vice versa
     * @param nullable
     * @return
     */
    def setNullableForAllColumns(nullable: Boolean): DataFrame = {
      def loop(s: StructType): Seq[StructField] =
        s.map {
          case StructField(name, dataType: StructType, _, metadata) =>
            StructField(
              name,
              StructType(loop(dataType)),
              nullable,
              metadata
            )
          case StructField(name, dataType: ArrayType, _, metadata) if dataType.elementType.isInstanceOf[StructType] =>
            StructField(
              name,
              ArrayType(StructType(loop(dataType.elementType.asInstanceOf[StructType]))),
              nullable,
              metadata
            )
          case t @ StructField(_, _, _, _) => t.copy(nullable = nullable)
        }
      df.sqlContext.createDataFrame(
        df.rdd,
        StructType(loop(df.schema))
      )
    }

    /**
     * Sorts this DataFrame columns order according to the Ordering which results from transforming
     * an implicitly given Ordering with a transformation function.
     * This function will also sort [[StructType]] columns and [[ArrayType]]([[StructType]]) columns.
     *  @see [[scala.math.Ordering]]
     *  @param   f the transformation function mapping elements of type [[StructField]]
     *           to some other domain `A`.
     *  @param   ord the ordering assumed on domain `A`.
     *  @tparam  A the target type of the transformation `f`, and the type where
     *           the ordering `ord` is defined.
     *  @return  a DataFrame consisting of the fields of this DataFrame
     *           sorted according to the ordering where `x < y` if
     *           `ord.lt(f(x), f(y))`.
     *
     * @example {{{
     *   // Example DataFrame
     *   val df = spark.createDataFrame(
     *     Seq(
     *       ("John", 30, 2000.0),
     *       ("Jane", 25, 3000.0)
     *     )
     *   ).toDF("name", "age", "salary")
     *
     *   // Sort columns by name
     *   val sortedByNameDF = df.sortColumnsBy(_.name)
     *   sortedByNameDF.show()
     *   // Output:
     *   // +---+----+------+
     *   // |age|name|salary|
     *   // +---+----+------+
     *   // | 30|John|2000.0|
     *   // | 25|Jane|3000.0|
     *   // +---+----+------+
     * }}}
     */
    def sortColumnsBy[A](f: StructField => A)(implicit ord: Ordering[A]): DataFrame =
      df
        .select(df.schema.toSortedSelectExpr(f): _*)
  }
}

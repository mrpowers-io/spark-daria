package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField

private[sql] case class DataFrameColumnsException(smth: String) extends Exception(smth)

object DataFrameExt {

  implicit class DataFrameMethods(df: DataFrame) {

    /** Prints the schema with StructType and StructFields so it's easy to copy into code */
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

    /** Executes a list of custom DataFrame transformations */
    def composeTransforms(
      transforms: List[(DataFrame => DataFrame)]
    ): DataFrame = {
      transforms.foldLeft(df) { (memoDF, t) =>
        memoDF.transform(t)
      }
    }

    /** Reorders columns as specified */
    def reorderColumns(colNames: Seq[String]): DataFrame = {
      val cols = colNames.map(col(_))
      df.select(cols: _*)
    }

    /** Returns true if the DataFrame contains the column */
    def containsColumn(colName: String): Boolean = {
      df.schema.fieldNames.contains(colName)
    }

    /** Returns the columns in otherDF that aren't in self */
    def columnDiff(otherDF: DataFrame): Seq[String] = {
      (df.columns).diff(otherDF.columns).toSeq
    }

    /** Like transform(), but for CustomTransform objects */
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

  }

}


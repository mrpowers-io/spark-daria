package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField

case class DataFrameColumnsException(smth: String) extends Exception(smth)

object DataFrameExt {

  implicit class DataFrameMethods(df: DataFrame) {

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

    def composeTransforms(
      transforms: List[(DataFrame => DataFrame)]
    ): DataFrame = {
      transforms.foldLeft(df) { (memoDF, t) =>
        memoDF.transform(t)
      }
    }

    def reorderColumns(colNames: Seq[String]): DataFrame = {
      val cols = colNames.map(col(_))
      df.select(cols: _*)
    }

    def containsColumn(colName: String): Boolean = {
      df.schema.fieldNames.contains(colName)
    }

    def columnDiff(otherDF: DataFrame): Seq[String] = {
      (df.columns).diff(otherDF.columns).toSeq
    }

    def trans(customTransform: CustomTransform): DataFrame = {
      if (df.columns.toSeq.exists((c: String) => customTransform.columnsAdded.contains(c))) {
        throw new DataFrameColumnsException(s"The DataFrame already contains the columns your transformation will add. The DataFrame has these columns: [${df.columns.mkString(", ")}]. You've asserted that your transformation will add these columns: [${customTransform.columnsAdded.mkString(", ")}]")
      }

      if (!customTransform.columnsRemoved.isEmpty && df.columns.toSeq.intersect(customTransform.columnsRemoved).isEmpty) {
        throw new DataFrameColumnsException(s"The DataFrame does not contain the columns your transformation will drop. The DataFrame has these columns: [${df.columns.mkString(", ")}]. You've asserted that your transformation will drop these columns: [${customTransform.columnsRemoved.mkString(", ")}]")
      }

      val transformedDF = df.transform(customTransform.transform)

      val actualColumnsAdded = transformedDF.columnDiff(df)
      if (!actualColumnsAdded.equals(customTransform.columnsAdded)) {
        throw new DataFrameColumnsException(s"The [${actualColumnsAdded.mkString(", ")}] columns were actually added, but you specified that these columns should have been added [${customTransform.columnsAdded.mkString(", ")}]")
      }

      val actualColumnsRemoved = df.columnDiff(transformedDF)
      if (!actualColumnsRemoved.equals(customTransform.columnsRemoved)) {
        throw new DataFrameColumnsException(s"The [${actualColumnsRemoved.mkString(", ")}] columns were actually removed, but you specified that these columns should have been removed [${customTransform.columnsRemoved.mkString(", ")}]")
      }

      transformedDF
    }

  }

}


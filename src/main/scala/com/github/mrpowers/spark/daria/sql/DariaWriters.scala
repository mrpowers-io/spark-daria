package com.github.mrpowers.spark.daria.sql

import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, DataFrame}
import scala.util.Try
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.IOUtils
import java.io.IOException
import com.github.mrpowers.spark.daria.hadoop.FsHelpers

object DariaWriters {

  // tmpFolder should look like s3a://bucket/data/src
  // filename should look like s3a://bucket/data/dest/my_cool_file.csv
  def writeSingleFile(
      df: DataFrame,             // must be small
      format: String = "csv",    // csv, parquet
      sc: SparkContext,          // pass in spark.sparkContext
      tmpFolder: String,         // will be deleted, so make sure it doesn't already exist
      filename: String,          // the full filename you want outputted
      saveMode: String = "error" // Spark default is error, overwrite and append are also common
  ): Unit = {
    df.repartition(1)
      .write
      .mode(saveMode)
      .format(format)
      .save(tmpFolder)
    val conf    = sc.hadoopConfiguration
    val src     = new Path(tmpFolder)
    val fs      = src.getFileSystem(conf)
    val oneFile = fs.listStatus(src).map(x => x.getPath.toString()).find(x => x.endsWith(format))
    val srcFile = new Path(oneFile.getOrElse(""))
    val dest    = new Path(filename)
    fs.rename(srcFile, dest)
  }

  def writeThenMerge(
      df: DataFrame,
      format: String = "csv",                // csv, parquet
      sc: SparkContext,                      // pass in spark.sparkContext
      tmpFolder: String,                     // will be deleted, so make sure it doesn't already exist
      filename: String,                      // the full filename you want outputted
      saveModeForTmpFolder: String = "error" // Spark default is error, overwrite and append are also common
  ): Unit = {
    df.write
      .mode(saveModeForTmpFolder)
      .format(format)
      .save(tmpFolder)
    FsHelpers.dariaCopyMerge(tmpFolder, filename, sc)
  }

}

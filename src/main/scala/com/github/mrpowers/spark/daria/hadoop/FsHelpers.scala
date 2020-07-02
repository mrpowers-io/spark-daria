package com.github.mrpowers.spark.daria.hadoop

import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, DataFrame}
import scala.util.Try
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.IOUtils
import java.io.IOException

object FsHelpers {

  // based on this answer: https://stackoverflow.com/a/50545815/1125159
  def dariaCopyMerge(
      srcPath: String,
      dstPath: String,
      sc: SparkContext,
      deleteSource: Boolean = true
  ): Boolean = {
    val hadoopConfig = sc.hadoopConfiguration
    val hdfs         = FileSystem.get(hadoopConfig)
    val s            = new Path(srcPath)
    val d            = new Path(dstPath)

    if (hdfs.exists(d))
      hdfs.delete(d, true)

    // Source path is expected to be a directory:
    if (hdfs.getFileStatus(s).isDirectory()) {

      val outputFile = hdfs.create(d)
      Try {
        hdfs
          .listStatus(s)
          .sortBy(_.getPath.getName)
          .collect {
            case status if status.isFile() =>
              val inputFile = hdfs.open(status.getPath())
              Try(IOUtils.copyBytes(inputFile, outputFile, hadoopConfig, false))
              inputFile.close()
          }
      }
      outputFile.close()

      if (deleteSource) hdfs.delete(s, true) else true
    } else {
      false
    }
  }

}

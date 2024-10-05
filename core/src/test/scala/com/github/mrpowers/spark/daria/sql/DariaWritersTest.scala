package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.{SaveMode, DataFrame}
//import org.apache.hadoop.fs.{FileSystem, Path}

import utest._

object DariaWritersTest extends TestSuite with SparkSessionTestWrapper {

  val tests = Tests {

    'writeSingleFile - {
      //val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

      val sourceDataPath = new java.io.File("./src/test/resources/some_data.csv").getCanonicalPath
      val df = spark.read
        .option("header", "true")
        .option("charset", "UTF8")
        .csv(sourceDataPath)

      val multiPartitionDF = df.repartition(5) // this would get written out as 5 files by default
      val tmpFolder        = new java.io.File("./tmp/daria-writer").getCanonicalPath
      val filename         = new java.io.File("./tmp/my-wonderful-file.csv").getCanonicalPath

      DariaWriters.writeSingleFile(
        df = multiPartitionDF,
        format = "csv",
        sc = spark.sparkContext,
        tmpFolder = tmpFolder,
        filename = filename,
        saveMode = "overwrite"
      )

      //val parquetDataPath = new java.io.File("./tmp/parquet-data").getCanonicalPath

      //val p = new Path(parquetDataPath)
      //if (fs.exists(new Path(parquetDataPath))) {
      //fs.delete(new Path(parquetDataPath), true)
      //}

      //df.repartition(50).write.parquet(parquetDataPath)

      //val preCompactionCount = spark.read.parquet(parquetDataPath).count()

      //val c = new ParquetCompactor(
      //dirname = parquetDataPath,
      //numOutputFiles = 10
      //)

      //c.run()

      //val postCompactionCount = spark.read.parquet(parquetDataPath).count()

      //assert(preCompactionCount == postCompactionCount)
    }

    'writeThenMerge - {
      //val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val sourceDataPath = new java.io.File("./src/test/resources/some_data.csv").getCanonicalPath
      val df = spark.read
        .option("header", "true")
        .option("charset", "UTF8")
        .csv(sourceDataPath)
      val multiPartitionDF = df.repartition(5) // this would get written out as 5 files by default
      val tmpFolder        = new java.io.File("./tmp/daria-writer-merge").getCanonicalPath
      val filename         = new java.io.File("./tmp/my-merged-file.csv").getCanonicalPath
      DariaWriters.writeThenMerge(
        df = multiPartitionDF,
        format = "csv",
        sc = spark.sparkContext,
        tmpFolder = tmpFolder,
        filename = filename,
        saveModeForTmpFolder = "overwrite"
      )
    }

  }

}

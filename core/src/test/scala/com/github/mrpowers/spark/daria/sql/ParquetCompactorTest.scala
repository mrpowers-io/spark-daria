package com.github.mrpowers.spark.daria.sql
import org.apache.hadoop.fs.{FileSystem, Path}

import utest._

object ParquetCompactorTest extends TestSuite with SparkSessionTestWrapper {

  val tests = Tests {

    'compact - {
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

      val sourceDataPath = new java.io.File("./src/test/resources/some_data.csv").getCanonicalPath
      val df = spark.read
        .option("header", "true")
        .option("charset", "UTF8")
        .csv(sourceDataPath)

      val parquetDataPath = new java.io.File("./tmp/parquet-data").getCanonicalPath

      val p = new Path(parquetDataPath)
      if (fs.exists(new Path(parquetDataPath))) {
        fs.delete(new Path(parquetDataPath), true)
      }

      df.repartition(50).write.parquet(parquetDataPath)

      val preCompactionCount = spark.read.parquet(parquetDataPath).count()

      val c = new ParquetCompactor(
        dirname = parquetDataPath,
        numOutputFiles = 10
      )

      c.run()

      val postCompactionCount = spark.read.parquet(parquetDataPath).count()

      assert(preCompactionCount == postCompactionCount)
    }

  }

}

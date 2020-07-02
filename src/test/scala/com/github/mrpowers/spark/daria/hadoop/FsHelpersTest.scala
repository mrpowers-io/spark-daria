package com.github.mrpowers.spark.daria.hadoop

import utest._

object FsHelpersTest extends TestSuite with com.github.mrpowers.spark.daria.sql.SparkSessionTestWrapper {

  val tests = Tests {

    'dariaCopyMerge - {

      "works with csv files" - {

        val srcPath = new java.io.File("./src/test/resources/csvs").getCanonicalPath
        val dstPath = new java.io.File("./tmp/merged_files.csv").getCanonicalPath

        FsHelpers.dariaCopyMerge(
          srcPath = srcPath,
          dstPath = dstPath,
          sc = spark.sparkContext,
          deleteSource = false
        )

      }

    }

  }

}

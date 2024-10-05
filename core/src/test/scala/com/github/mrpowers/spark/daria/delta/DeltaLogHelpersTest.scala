package com.github.mrpowers.spark.daria.delta

import utest._
import com.github.mrpowers.spark.daria.sql.SparkSessionTestWrapper
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DeltaLogHelpersTest extends TestSuite with DataFrameComparer with SparkSessionTestWrapper {

  val tests = Tests {

    'num1GbPartitions - {

      "returns the number of 1GB partitions in a Delta Lake" - {

        val df = spark.createDF(
          List(
            ("some_file.snappy.parquet", 940L)
          ),
          List(
            ("path", StringType, true),
            ("size", LongType, true)
          )
        )

        assert(DeltaLogHelpers.num1GbPartitions(df) == 1)

      }

      "reverts to the minimum if the data size is small" - {

        val df = spark.createDF(
          List(
            ("some_file.snappy.parquet", 940L)
          ),
          List(
            ("path", StringType, true),
            ("size", LongType, true)
          )
        )

        assert(DeltaLogHelpers.num1GbPartitions(df, 200) == 200)

      }

      "returns a big number when there is a lot of data" - {

        val df = spark.createDF(
          List(
            ("some_file.snappy.parquet", 940L),
            ("some_file.snappy.parquet", 234234234940L),
            ("some_file.snappy.parquet", 2390234092840L)
          ),
          List(
            ("path", StringType, true),
            ("size", LongType, true)
          )
        )

        assert(DeltaLogHelpers.num1GbPartitions(df) == 2444)

      }

    }

    'partitionedLake1GbChunks - {

      "it computes the 1GB chunks per partition" - {

        val df = spark.createDF(
          List(
            ("some_file.snappy.parquet", Map("country" -> "China"), 234234234940L),
            ("some_file.snappy.parquet", Map("country" -> "China"), 1000L),
            ("some_file.snappy.parquet", Map("country" -> "India"), 50L),
            ("some_file.snappy.parquet", Map("country" -> "Japan"), 50L)
          ),
          List(
            ("path", StringType, true),
            ("partitionValues", MapType(StringType, StringType, true), true),
            ("size", LongType, true)
          )
        )

        val expectedDF = spark.createDF(
          List(
            ("China", 218),
            ("India", 1),
            ("Japan", 1)
          ),
          List(
            ("partitionValue", StringType, true),
            ("num_1GB_partitions", IntegerType, true)
          )
        )

        assertSmallDataFrameEquality(
          DeltaLogHelpers.partitionedLake1GbChunks(df, "country").select("partitionValue", "num_1GB_partitions"),
          expectedDF
        )

      }

    }

  }

}

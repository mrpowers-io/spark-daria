package com.github.mrpowers.spark.daria.sql

import utest._
import org.apache.spark.sql.types._
import SparkSessionExt._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer

object DataFrameHelpersTest extends TestSuite with SparkSessionTestWrapper with DataFrameComparer {

  val tests = Tests {

    'twoColumnsToMap - {
      "converts two columns of a DataFrame to a map" - {
        val sourceDF = spark.createDF(
          List(
            ("boracay", 7),
            ("long island", 9)
          ),
          List(
            ("island", StringType, true),
            ("fun_level", IntegerType, true)
          )
        )
        val actual =
          DataFrameHelpers.twoColumnsToMap[String, Int](
            sourceDF,
            "island",
            "fun_level"
          )
        val expected = Map(
          "boracay"     -> 7,
          "long island" -> 9
        )
        actual ==> expected
      }
    }

    'columnToArray - {
      "converts a column to an array" - {
        val sourceDF =
          spark.createDF(
            List(
              1,
              2,
              3
            ),
            List(
              ("num", IntegerType, true)
            )
          )
        val actual = DataFrameHelpers.columnToArray[Int](sourceDF, "num")
        actual ==> Array(1, 2, 3)
      }
    }

    'columnToList - {
      "converts a column to a list" - {
        val sourceDF =
          spark.createDF(
            List(
              1,
              2,
              3
            ),
            List(
              ("num", IntegerType, true)
            )
          )
        val actual = DataFrameHelpers.columnToList[Int](sourceDF, "num")
        actual ==> List(1, 2, 3)
      }
    }

    'toArrayOfMaps - {
      "converts a DataFrame into an array of maps" - {
        val sourceDF =
          spark.createDF(
            List(
              ("doctor", 4, "high"),
              ("dentist", 10, "high")
            ),
            List(
              ("profession", StringType, true),
              ("some_number", IntegerType, true),
              ("pay_grade", StringType, true)
            )
          )
        val actual = DataFrameHelpers.toArrayOfMaps(sourceDF)
        val expected = Array(
          Map(
            "profession"  -> "doctor",
            "some_number" -> 4,
            "pay_grade"   -> "high"
          ),
          Map(
            "profession"  -> "dentist",
            "some_number" -> 10,
            "pay_grade"   -> "high"
          )
        )
        actual ==> expected
      }
    }

    'printAthenaCreateTable - {
      "prints the CREATE TABLE Athena code" - {
        val df = spark.createDF(
          List(
            ("jets", "football", 45),
            ("nacional", "soccer", 10)
          ),
          List(
            ("team", StringType, true),
            ("sport", StringType, true),
            ("goals_for", IntegerType, true)
          )
        )
        //      uncomment the next line if you want to check out the console output
//        DataFrameHelpers.printAthenaCreateTable(
//          df,
//          "my_cool_athena_table",
//          "s3://my-bucket/extracts/people"
//        )
      }
    }

//    'writeTimestamped - {
//
//      "writes out timestamped data and a latest file" - {
//
//        val df = spark.createDF(
//          List(
//            ("giants", "football", 45),
//            ("nacional", "soccer", 10)
//          ),
//          List(
//            ("team", StringType, true),
//            ("sport", StringType, true),
//            ("goals_for", IntegerType, true)
//          )
//        )
//
//        DataFrameHelpers.writeTimestamped(
//          df,
//          "/Users/powers/Documents/tmp/daria_timestamp_ex",
//          numPartitions = Some(3),
//          overwriteLatest = true
//        )
//
//      }
//
//    }
//
//    'readTimestamped - {
//
//      val df = DataFrameHelpers.readTimestamped(
//        "/Users/powers/Documents/tmp/daria_timestamp_ex"
//      )
//      df.show()
//
//    }

    "toCreateDataFrameCode" - {
      val df1 = spark.createDF(
        List(
          ("boracay", 7, 3.4, 4L),
          ("long island", 9, null, 5L)
        ),
        List(
          ("island", StringType, true),
          ("fun_level", IntegerType, true),
          ("some_double", DoubleType, true),
          ("some_long", LongType, true)
        )
      )
      val res = DataFrameHelpers.toCreateDataFrameCode(df1)
      val expected =
        """val data = Seq(
          |  Row("boracay", 7, 3.4, 4L),
          |  Row("long island", 9, null, 5L))
          |val schema = StructType(Seq(
          |  StructField("island", StringType, true),
          |  StructField("fun_level", IntegerType, true),
          |  StructField("some_double", DoubleType, true),
          |  StructField("some_long", LongType, true)))
          |val df = spark.createDataFrame(
          |  spark.sparkContext.parallelize(data),
          |  schema
          |)
          |""".stripMargin
      assert(res == expected)
    }

  }

}

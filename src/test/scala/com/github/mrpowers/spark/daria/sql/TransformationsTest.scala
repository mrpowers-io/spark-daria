package com.github.mrpowers.spark.daria.sql

import utest._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.github.mrpowers.spark.fast.tests.ColumnComparer
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._

object TransformationsTest extends TestSuite with DataFrameComparer with ColumnComparer with SparkSessionTestWrapper {

  val tests = Tests {

    'sortColumns - {

      "sorts the columns in a DataFrame in ascending order" - {

        val sourceDF = spark.createDF(
          List(("pablo", 3, "polo")),
          List(
            ("name", StringType, true),
            ("age", IntegerType, true),
            ("sport", StringType, true)
          )
        )

        val actualDF = sourceDF.transform(transformations.sortColumns())

        val expectedDF = spark.createDF(
          List((3, "pablo", "polo")),
          List(
            ("age", IntegerType, true),
            ("name", StringType, true),
            ("sport", StringType, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

      "sorts the columns in a DataFrame in descending order" - {

        val sourceDF = spark.createDF(
          List(("pablo", 3, "polo")),
          List(
            ("name", StringType, true),
            ("age", IntegerType, true),
            ("sport", StringType, true)
          )
        )

        val actualDF = sourceDF.transform(transformations.sortColumns("desc"))

        val expectedDF = spark.createDF(
          List(("polo", "pablo", 3)),
          List(
            ("sport", StringType, true),
            ("name", StringType, true),
            ("age", IntegerType, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

      "throws an error if the sort order is invalid" - {

        val sourceDF = spark.createDF(
          List(("pablo", 3, "polo")),
          List(
            ("name", StringType, true),
            ("age", IntegerType, true),
            ("sport", StringType, true)
          )
        )

        val e = intercept[InvalidColumnSortOrderException] {
          sourceDF.transform(transformations.sortColumns("cats"))
        }

      }

    }

    'snakeCaseColumns - {

      "snake_cases the columns of a DataFrame" - {

        val sourceDF = spark.createDF(
          List(("funny", "joke", "person")),
          List(
            ("A b C", StringType, true),
            ("de F", StringType, true),
            ("cr   ay", StringType, true)
          )
        )

        val actualDF = sourceDF.transform(transformations.snakeCaseColumns())

        val expectedDF = spark.createDF(
          List(("funny", "joke", "person")),
          List(
            ("a_b_c", StringType, true),
            ("de_f", StringType, true),
            ("cr_ay", StringType, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

    }

    'snakifyColumns - {

      val sourceDF = spark.createDF(
        List(("funny", "joke")),
        List(
          ("ThIs", StringType, true),
          ("BiH", StringType, true)
        )
      )

      val actualDF = sourceDF.transform(transformations.snakifyColumns())

      val expectedDF = spark.createDF(
        List(("funny", "joke")),
        List(
          ("th_is", StringType, true),
          ("bi_h", StringType, true)
        )
      )

      assertSmallDataFrameEquality(
        actualDF,
        expectedDF
      )

    }

    'camelCaseToSnakeCaseColumns - {
      "convert camel case columns to snake case" - {

        val df = spark
          .createDF(
            List(
              ("John", 1, 101),
              ("Paul", 2, 102),
              ("Jane", 3, 103)
            ),
            List(
              ("userName", StringType, true),
              ("userId", IntegerType, true),
              ("internalUserId", IntegerType, true)
            )
          )
          .transform(transformations.camelCaseToSnakeCaseColumns())

        val expectedDF = spark
          .createDF(
            List(
              ("John", 1, 101),
              ("Paul", 2, 102),
              ("Jane", 3, 103)
            ),
            List(
              ("user_name", StringType, true),
              ("user_id", IntegerType, true),
              ("internal_user_id", IntegerType, true)
            )
          )
        assert(df.columns.toSet == expectedDF.columns.toSet)
      }
    }

    'titleCaseColumns - {

      "Title Case the columns of a DataFrame" - {

        val sourceDF =
          spark.createDF(
            List(("funny", "joke")),
            List(
              ("This is a simple text", StringType, true),
              ("this is anoTher teXt", StringType, true)
            )
          )

        val actualDF = sourceDF.transform(transformations.titleCaseColumns())

        val expectedDF =
          spark.createDF(
            List(("funny", "joke")),
            List(
              ("This Is A Simple Text", StringType, true),
              ("This Is Another Text", StringType, true)
            )
          )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

    }

    "snakifyColumns catch-all scenarios" - {

      val sourceDF = spark.createDF(
        List(
          (
            "mixed case with spaces",
            "mixed case with spaces",
            "multi-spaces",
            "word with a capitalized letter",
            "BiH",
            "a_b_c",
            "de_f",
            "cr_ay",
            "col_four",
            "colfive",
            "col_sevenAh",
            "HOWABOUTTHIS",
            "thisOne",
            "ORACLE_CASING"
          )
        ),
        List(
          ("A b C", StringType, true),
          ("de F", StringType, true),
          ("cr   ay", StringType, true),
          ("ThIs", StringType, true),
          ("BiH", StringType, true),
          ("a_b_c", StringType, true),
          ("de_f", StringType, true),
          ("cr_ay", StringType, true),
          ("col_four", StringType, true),
          ("colfive", StringType, true),
          ("col_sevenAh", StringType, true),
          ("HOWABOUTTHIS", StringType, true),
          ("thisOne", StringType, true),
          ("oracle_casing", StringType, true)
        )
      )

      val actualDf = sourceDF.transform(transformations.toSnakeCaseColumns())

      val expectedDF = spark.createDF(
        List(
          (
            "mixed case with spaces",
            "mixed case with spaces",
            "multi-spaces",
            "word with a capitalized letter",
            "BiH",
            "a_b_c",
            "de_f",
            "cr_ay",
            "col_four",
            "colfive",
            "col_sevenAh",
            "HOWABOUTTHIS",
            "thisOne",
            "ORACLE_CASING"
          )
        ),
        List(
          ("a_b_c", StringType, true),
          ("de_f", StringType, true),
          ("cr_ay", StringType, true),
          ("th_is", StringType, true),
          ("bi_h", StringType, true),
          ("a_b_c", StringType, true),
          ("de_f", StringType, true),
          ("cr_ay", StringType, true),
          ("col_four", StringType, true),
          ("colfive", StringType, true),
          ("col_seven_ah", StringType, true),
          ("howaboutthis", StringType, true),
          ("this_one", StringType, true),
          ("oracle_casing", StringType, true)
        )
      )

      assertSmallDataFrameEquality(
        actualDf,
        expectedDF
      )

    }

    "camelCase catch-all scenarios" - {

      val sourceDF = spark.createDF(
        List(
          (
            "mixed case with spaces",
            "mixed case with spaces",
            "multi-spaces",
            "word with a capitalized letter",
            "BiH",
            "a_b_c",
            "de_f",
            "cr_ay",
            "col_four",
            "colfive",
            "col_sevenAh",
            "HOWABOUTTHIS",
            "thisOne",
            "ORACLE_CASING"
          )
        ),
        List(
          ("A b C", StringType, true),
          ("de F", StringType, true),
          ("cr   ay", StringType, true),
          ("ThIs", StringType, true),
          ("BiH", StringType, true),
          ("a_b_c", StringType, true),
          ("de_f", StringType, true),
          ("cr_ay", StringType, true),
          ("col_four", StringType, true),
          ("colfive", StringType, true),
          ("col_sevenAh", StringType, true),
          ("HOWABOUTTHIS", StringType, true),
          ("thisOne", StringType, true),
          ("oracle_casing", StringType, true)
        )
      )

      val actualDf = sourceDF.transform(transformations.camelCaseColumns())

      val expectedDF = spark.createDF(
        List(
          (
            "mixed case with spaces",
            "mixed case with spaces",
            "multi-spaces",
            "word with a capitalized letter",
            "BiH",
            "a_b_c",
            "de_f",
            "cr_ay",
            "col_four",
            "colfive",
            "col_sevenAh",
            "HOWABOUTTHIS",
            "thisOne",
            "ORACLE_CASING"
          )
        ),
        List(
          ("aBC", StringType, true),
          ("deF", StringType, true),
          ("crAy", StringType, true),
          ("thIs", StringType, true),
          ("biH", StringType, true),
          ("aBC", StringType, true),
          ("deF", StringType, true),
          ("crAy", StringType, true),
          ("colFour", StringType, true),
          ("colfive", StringType, true),
          ("colSevenAh", StringType, true),
          ("howaboutthis", StringType, true),
          ("thisOne", StringType, true),
          ("oracleCasing", StringType, true)
        )
      )

      assertSmallDataFrameEquality(
        actualDf,
        expectedDF
      )

    }

    "upperCase catch-all scenarios" - {

      val sourceDF = spark.createDF(
        List(
          (
            "mixed case with spaces",
            "mixed case with spaces",
            "multi-spaces",
            "word with a capitalized letter",
            "BiH",
            "a_b_c",
            "de_f",
            "cr_ay",
            "col_four",
            "colfive",
            "col_sevenAh",
            "HOWABOUTTHIS",
            "thisOne",
            "ORACLE_CASING"
          )
        ),
        List(
          ("A b C", StringType, true),
          ("de F", StringType, true),
          ("cr   ay", StringType, true),
          ("ThIs", StringType, true),
          ("BiH", StringType, true),
          ("a_b_c", StringType, true),
          ("de_f", StringType, true),
          ("cr_ay", StringType, true),
          ("col_four", StringType, true),
          ("colfive", StringType, true),
          ("col_sevenAh", StringType, true),
          ("HOWABOUTTHIS", StringType, true),
          ("thisOne", StringType, true),
          ("oracle_casing", StringType, true)
        )
      )

      val actualDf = sourceDF.transform(transformations.upperCaseColumns())

      val expectedDF = spark.createDF(
        List(
          (
            "mixed case with spaces",
            "mixed case with spaces",
            "multi-spaces",
            "word with a capitalized letter",
            "BiH",
            "a_b_c",
            "de_f",
            "cr_ay",
            "col_four",
            "colfive",
            "col_sevenAh",
            "HOWABOUTTHIS",
            "thisOne",
            "ORACLE_CASING"
          )
        ),
        List(
          ("A_B_C", StringType, true),
          ("DE_F", StringType, true),
          ("CR_AY", StringType, true),
          ("TH_IS", StringType, true),
          ("BI_H", StringType, true),
          ("A_B_C", StringType, true),
          ("DE_F", StringType, true),
          ("CR_AY", StringType, true),
          ("COL_FOUR", StringType, true),
          ("COLFIVE", StringType, true),
          ("COL_SEVEN_AH", StringType, true),
          ("HOWABOUTTHIS", StringType, true),
          ("THIS_ONE", StringType, true),
          ("ORACLE_CASING", StringType, true)
        )
      )

      assertSmallDataFrameEquality(
        actualDf,
        expectedDF
      )

    }

    'prependToColName - {

      "Prepends a string to all column names" - {

        val sourceDF =
          spark.createDF(
            List(("funny", "joke")),
            List(
              ("some col", StringType, true),
              ("another col", StringType, true)
            )
          )

        val actualDF = sourceDF.transform(transformations.prependToColName("hi "))

        val expectedDF =
          spark.createDF(
            List(("funny", "joke")),
            List(
              ("hi some col", StringType, true),
              ("hi another col", StringType, true)
            )
          )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

    }

    'multiRegexpReplace - {

      "remove characters that match a regular expression in the columns of a DataFrame" - {

        val sourceDF =
          spark.createDF(
            List(("\u0000StringTest", 123)),
            List(
              ("StringTypeCol", StringType, true),
              ("IntegerTypeCol", IntegerType, true)
            )
          )

        val actualDF = sourceDF.transform(
          transformations.multiRegexpReplace(
            List(col("StringTypeCol")),
            "\u0000",
            "ThisIsA"
          )
        )

        val expectedDF =
          spark.createDF(
            List(("ThisIsAStringTest", 123)),
            List(
              ("StringTypeCol", StringType, true),
              ("IntegerTypeCol", IntegerType, true)
            )
          )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

      "removes \\x00 in the columns of a DataFrame" - {

        val sourceDF = spark.createDF(
          List(("\\x00", 123)),
          List(
            ("StringTypeCol", StringType, true),
            ("num", IntegerType, true)
          )
        )

        val actualDF = sourceDF.transform(
          transformations.multiRegexpReplace(
            List(
              col("StringTypeCol"),
              col("num")
            ),
            "\\\\x00",
            ""
          )
        )

        val expectedDF =
          spark.createDF(
            List(("", "123")),
            List(
              ("StringTypeCol", StringType, true),
              ("num", StringType, true)
            )
          )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

      "works on multiple columns" - {

        val sourceDF = spark.createDF(
          List(
            ("Bart cool", "moto cool"),
            ("cool James", "droid fun")
          ),
          List(
            ("person", StringType, true),
            ("phone", StringType, true)
          )
        )

        val actualDF = sourceDF.transform(
          transformations.multiRegexpReplace(
            List(
              col("person"),
              col("phone")
            ),
            "cool",
            "dude"
          )
        )

        val expectedDF = spark.createDF(
          List(
            ("Bart dude", "moto dude"),
            ("dude James", "droid fun")
          ),
          List(
            ("person", StringType, true),
            ("phone", StringType, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

    }

    'bulkRegexpReplace - {

      "works on all the StringType columns" - {

        val sourceDF = spark.createDF(
          List(
            ("Bart cool", "moto cool", 5),
            ("cool James", "droid fun", 10)
          ),
          List(
            ("person", StringType, true),
            ("phone", StringType, true),
            ("num", IntegerType, true)
          )
        )

        val actualDF =
          sourceDF.transform(
            transformations.bulkRegexpReplace(
              "cool",
              "dude"
            )
          )

        val expectedDF = spark.createDF(
          List(
            ("Bart dude", "moto dude", 5),
            ("dude James", "droid fun", 10)
          ),
          List(
            ("person", StringType, true),
            ("phone", StringType, true),
            ("num", IntegerType, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

    }

    'truncateColumns - {

      "truncates columns based on specified lengths" - {

        val sourceDF = spark.createDF(
          List(
            ("Bart cool", "moto cool"),
            ("cool James", "droid fun"),
            (null, null)
          ),
          List(
            ("person", StringType, true),
            ("phone", StringType, true)
          )
        )

        val columnLengths: Map[String, Int] =
          Map(
            "person"   -> 2,
            "phone"    -> 3,
            "whatever" -> 50000
          )

        val actualDF =
          sourceDF.transform(transformations.truncateColumns(columnLengths))

        val expectedDF =
          spark.createDF(
            List(
              ("Ba", "mot"),
              ("co", "dro"),
              (null, null)
            ),
            List(
              ("person", StringType, true),
              ("phone", StringType, true)
            )
          )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

    }

    'withColBucket - {

      "buckets a numeric column" - {

        val df = spark
          .createDF(
            List(
              (20, "10-20"),
              (50, "41-50"),
              (10, "10-20"),
              (40, "31-40"),
              (50, "41-50"),
              (72, ">70"),
              (9, "<10"),
              (null, null),
              (62, null)
            ),
            List(
              ("some_num", IntegerType, true),
              ("expected", StringType, true)
            )
          )
          .transform(
            transformations.withColBucket(
              colName = "some_num",
              outputColName = "my_bucket",
              buckets = Array(
                (null, 10),
                (10, 20),
                (21, 30),
                (31, 40),
                (41, 50),
                (70, null)
              ),
              inclusiveBoundries = true
            )
          )

        assertColumnEquality(
          df,
          "expected",
          "my_bucket"
        )

      }

    }

    'extractFromJson - {

      val sourceDF = spark.createDF(
        List(
          (10, """{"name": "Bart cool", "age": 25}"""),
          (20, """{"name": "Lisa frost", "age": 27}""")
        ),
        List(
          ("id", IntegerType, true),
          ("person", StringType, true)
        )
      )

      'extractTheCompleteSchema - {
        val personSchema = StructType(
          List(
            StructField(
              "name",
              StringType
            ),
            StructField(
              "age",
              IntegerType
            )
          )
        )

        val actualDF = sourceDF.transform(
          transformations.extractFromJson(
            "person",
            "personData",
            personSchema
          )
        )

        val expectedDF = spark.createDF(
          List(
            (
              10,
              """{"name": "Bart cool", "age": 25}""",
              Row(
                "Bart cool",
                25
              )
            ),
            (
              20,
              """{"name": "Lisa frost", "age": 27}""",
              Row(
                "Lisa frost",
                27
              )
            )
          ),
          List(
            ("id", IntegerType, true),
            ("person", StringType, true),
            ("personData", personSchema, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )
      }

      'extractPartialSchema - {
        val personNameSchema = StructType(
          List(
            StructField(
              "name",
              StringType
            )
          )
        )

        val actualDF = sourceDF.transform(
          transformations.extractFromJson(
            "person",
            "personData",
            personNameSchema
          )
        )

        val expectedDF = spark.createDF(
          List(
            (10, """{"name": "Bart cool", "age": 25}""", Row("Bart cool")),
            (20, """{"name": "Lisa frost", "age": 27}""", Row("Lisa frost"))
          ),
          List(
            ("id", IntegerType, true),
            ("person", StringType, true),
            ("personData", personNameSchema, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )
      }

      'extractNonExistingSchema - {
        val wrongSchema =
          StructType(
            List(
              StructField(
                "wrong_field",
                StringType
              )
            )
          )

        val actualDF = sourceDF.transform(
          transformations.extractFromJson(
            "person",
            "personData",
            wrongSchema
          )
        )

        val expectedDF = spark.createDF(
          List(
            (10, """{"name": "Bart cool", "age": 25}""", Row(None.orNull)),
            (20, """{"name": "Lisa frost", "age": 27}""", Row(None.orNull))
          ),
          List(
            ("id", IntegerType, true),
            ("person", StringType, true),
            ("personData", wrongSchema, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )
      }
    }

    'getJsonObject - {
      val bart = """
                   |{
                   |  "name": "Bart cool",
                   |  "info": {
                   |    "age": 25,
                   |    "gender": "male"
                   |  }
                   |}
                 """.stripMargin

      val lisa = """
                   |{
                   |  "name": "Lisa frost",
                   |  "info": {
                   |    "age": 27,
                   |    "gender": "female"
                   |  }
                   |}
                 """.stripMargin

      val sourceDF = spark.createDF(
        List(
          (10, bart),
          (20, lisa)
        ),
        List(
          ("id", IntegerType, true),
          ("person", StringType, true)
        )
      )

      'fromOneLevelPath - {
        val actualDF = sourceDF.transform(
          transformations.extractFromJson(
            "person",
            "name",
            "$.name"
          )
        )

        val expectedDF = spark.createDF(
          List(
            (10, bart, "Bart cool"),
            (20, lisa, "Lisa frost")
          ),
          List(
            ("id", IntegerType, true),
            ("person", StringType, true),
            ("name", StringType, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )
      }

      'fromTwoLevelPath - {
        val actualDF = sourceDF.transform(
          transformations.extractFromJson(
            "person",
            "age",
            "$.info.age"
          )
        )

        val expectedDF = spark.createDF(
          List(
            (10, bart, "25"),
            (20, lisa, "27")
          ),
          List(
            ("id", IntegerType, true),
            ("person", StringType, true),
            ("age", StringType, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )
      }

      'fromNonExistingPath - {
        val actualDF = sourceDF.transform(
          transformations.extractFromJson(
            "person",
            "age",
            "$.age"
          )
        )

        val expectedDF = spark.createDF(
          List(
            (10, bart, null),
            (20, lisa, null)
          ),
          List(
            ("id", IntegerType, true),
            ("person", StringType, true),
            ("age", StringType, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )
      }
    }

    'withRowAsStruct - {
      "collects an entire row intro a StructType column" - {
        val sourceDF = spark
          .createDF(
            List(
              ("pablo", 3, "polo", 1.0),
              (null, 3, "polo", 5.5)
            ),
            List(
              ("name", StringType, true),
              ("age", IntegerType, true),
              ("sport", StringType, true),
              ("a_number", DoubleType, true)
            )
          )
          .transform(transformations.withRowAsStruct())

        //        sourceDF.show()

        // HACK - don't have a good way to test this yet
        // Need to add assertStructTypeColumnEquality: https://github.com/MrPowers/spark-fast-tests/issues/38
        // Or update assertSmallDataFrameEquality to work with DataFrames that have StructType columns: https://github.com/MrPowers/spark-fast-tests/issues/37
      }
    }

    'withParquetCompatibleColumnNames - {
      "blows up if the column name is invalid for Parquet" - {
        val df = spark
          .createDF(
            List(
              ("pablo")
            ),
            List(
              ("Column That {Will} Break\t;", StringType, true)
            )
          )
        val path = new java.io.File("./tmp/blowup/example").getCanonicalPath
        val e = intercept[org.apache.spark.sql.AnalysisException] {
          df.write.parquet(path)
        }
      }

      "converts column names to be Parquet compatible" - {
        val actualDF = spark
          .createDF(
            List(
              ("pablo")
            ),
            List(
              ("Column That {Will} Break\t;", StringType, true)
            )
          )
          .transform(transformations.withParquetCompatibleColumnNames())

        val expectedDF = spark.createDF(
          List(
            ("pablo")
          ),
          List(
            ("column_that_will_break", StringType, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )
      }
    }

  }

}

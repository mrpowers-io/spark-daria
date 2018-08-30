package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import utest._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.github.mrpowers.spark.fast.tests.ColumnComparer
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.Row

object TransformationsTest
  extends TestSuite
    with DataFrameComparer
    with ColumnComparer
    with SparkSessionTestWrapper {

  val tests = Tests {

    'sortColumns - {

      "sorts the columns in a DataFrame in ascending order" - {

        val sourceDF = spark.createDF(
          List(
            ("pablo", 3, "polo")
          ), List(
            ("name", StringType, true),
            ("age", IntegerType, true),
            ("sport", StringType, true)
          )
        )

        val actualDF = sourceDF.transform(transformations.sortColumns())

        val expectedDF = spark.createDF(
          List(
            (3, "pablo", "polo")
          ), List(
            ("age", IntegerType, true),
            ("name", StringType, true),
            ("sport", StringType, true)
          )
        )

        assertSmallDataFrameEquality(actualDF, expectedDF)

      }

      "sorts the columns in a DataFrame in descending order" - {

        val sourceDF = spark.createDF(
          List(
            ("pablo", 3, "polo")
          ), List(
            ("name", StringType, true),
            ("age", IntegerType, true),
            ("sport", StringType, true)
          )
        )

        val actualDF = sourceDF.transform(transformations.sortColumns("desc"))

        val expectedDF = spark.createDF(
          List(
            ("polo", "pablo", 3)
          ), List(
            ("sport", StringType, true),
            ("name", StringType, true),
            ("age", IntegerType, true)
          )
        )

        assertSmallDataFrameEquality(actualDF, expectedDF)

      }

      "throws an error if the sort order is invalid" - {

        val sourceDF = spark.createDF(
          List(
            ("pablo", 3, "polo")
          ), List(
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
          List(
            ("funny", "joke", "person")
          ), List(
            ("A b C", StringType, true),
            ("de F", StringType, true),
            ("cr   ay", StringType, true)
          )
        )

        val actualDF = sourceDF.transform(transformations.snakeCaseColumns())

        val expectedDF = spark.createDF(
          List(
            ("funny", "joke", "person")
          ), List(
            ("a_b_c", StringType, true),
            ("de_f", StringType, true),
            ("cr_ay", StringType, true)
          )
        )

        assertSmallDataFrameEquality(actualDF, expectedDF)

      }

    }

    'titleCaseColumns - {

      "Title Case the columns of a DataFrame" - {

        val sourceDF = spark.createDF(
          List(
            ("funny", "joke")
          ), List(
            ("This is a simple text", StringType, true),
            ("this is anoTher teXt", StringType, true)
          )
        )

        val actualDF = sourceDF.transform(transformations.titleCaseColumns())

        val expectedDF = spark.createDF(
          List(
            ("funny", "joke")
          ), List(
            ("This Is A Simple Text", StringType, true),
            ("This Is Another Text", StringType, true)
          )
        )

        assertSmallDataFrameEquality(actualDF, expectedDF)

      }

    }

    'multiRegexpReplace - {

      "remove characters that match a regular expression in the columns of a DataFrame" - {

        val sourceDF = spark.createDF(
          List(
            ("\u0000StringTest", 123)
          ), List(
            ("StringTypeCol", StringType, true),
            ("IntegerTypeCol", IntegerType, true)
          )
        )

        val actualDF = sourceDF.transform(
          transformations.multiRegexpReplace(
            List(
              col("StringTypeCol")
            ),
            "\u0000",
            "ThisIsA"
          )
        )

        val expectedDF = spark.createDF(
          List(
            ("ThisIsAStringTest", 123)
          ), List(
            ("StringTypeCol", StringType, true),
            ("IntegerTypeCol", IntegerType, true)
          )
        )

        assertSmallDataFrameEquality(actualDF, expectedDF)

      }

      "removes \\x00 in the columns of a DataFrame" - {

        val sourceDF = spark.createDF(
          List(
            ("\\x00", 123)
          ), List(
            ("StringTypeCol", StringType, true),
            ("num", IntegerType, true)
          )
        )

        val actualDF = sourceDF.transform(
          transformations.multiRegexpReplace(
            List(col("StringTypeCol"), col("num")),
            "\\\\x00",
            ""
          )
        )

        val expectedDF = spark.createDF(
          List(
            ("", "123")
          ), List(
            ("StringTypeCol", StringType, true),
            ("num", StringType, true)
          )
        )

        assertSmallDataFrameEquality(actualDF, expectedDF)

      }

      "works on multiple columns" - {

        val sourceDF = spark.createDF(
          List(
            ("Bart cool", "moto cool"),
            ("cool James", "droid fun")
          ), List(
            ("person", StringType, true),
            ("phone", StringType, true)
          )
        )

        val actualDF = sourceDF.transform(
          transformations.multiRegexpReplace(
            List(col("person"), col("phone")),
            "cool",
            "dude"
          )
        )

        val expectedDF = spark.createDF(
          List(
            ("Bart dude", "moto dude"),
            ("dude James", "droid fun")
          ), List(
            ("person", StringType, true),
            ("phone", StringType, true)
          )
        )

        assertSmallDataFrameEquality(actualDF, expectedDF)

      }

    }

    'bulkRegexpReplace - {

      "works on all the StringType columns" - {

        val sourceDF = spark.createDF(
          List(
            ("Bart cool", "moto cool", 5),
            ("cool James", "droid fun", 10)
          ), List(
            ("person", StringType, true),
            ("phone", StringType, true),
            ("num", IntegerType, true)
          )
        )

        val actualDF = sourceDF.transform(
          transformations.bulkRegexpReplace(
            "cool",
            "dude"
          )
        )

        val expectedDF = spark.createDF(
          List(
            ("Bart dude", "moto dude", 5),
            ("dude James", "droid fun", 10)
          ), List(
            ("person", StringType, true),
            ("phone", StringType, true),
            ("num", IntegerType, true)
          )
        )

        assertSmallDataFrameEquality(actualDF, expectedDF)

      }

    }

    'truncateColumns - {

      "truncates columns based on specified lengths" - {

        val sourceDF = spark.createDF(
          List(
            ("Bart cool", "moto cool"),
            ("cool James", "droid fun"),
            (null, null)
          ), List(
            ("person", StringType, true),
            ("phone", StringType, true)
          )
        )

        val columnLengths: Map[String, Int] = Map(
          "person" -> 2,
          "phone" -> 3,
          "whatever" -> 50000
        )

        val actualDF = sourceDF.transform(
          transformations.truncateColumns(columnLengths)
        )

        val expectedDF = spark.createDF(
          List(
            ("Ba", "mot"),
            ("co", "dro"),
            (null, null)
          ), List(
            ("person", StringType, true),
            ("phone", StringType, true)
          )
        )

        assertSmallDataFrameEquality(actualDF, expectedDF)

      }

    }

    'withColBucket - {

      "buckets a numeric column" - {

        val df = spark.createDF(
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
        ).transform(
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

        assertColumnEquality(df, "expected", "my_bucket")

      }

    }

    'extractFromJson - {

      val sourceDF = spark.createDF(
        List(
          (10, """{"name": "Bart cool", "age": 25}"""),
          (20, """{"name": "Lisa frost", "age": 27}""")
        ), List(
          ("id", IntegerType, true),
          ("person", StringType, true)
        )
      )

      'extractTheCompleteSchema - {
        val personSchema = StructType(List(
          StructField("name", StringType),
          StructField("age", IntegerType)
        ))

        val actualDF = sourceDF.transform(
          transformations.extractFromJson("person", "personData", personSchema)
        )

        val expectedDF = spark.createDF(
          List(
            (10, """{"name": "Bart cool", "age": 25}""", Row("Bart cool", 25)),
            (20, """{"name": "Lisa frost", "age": 27}""", Row("Lisa frost", 27))
          ), List(
            ("id", IntegerType, true),
            ("person", StringType, true),
            ("personData", personSchema, true)
          )
        )

        assertSmallDataFrameEquality(actualDF, expectedDF)
      }

      'extractPartialSchema - {
        val personNameSchema = StructType(List(
          StructField("name", StringType)
        ))

        val actualDF = sourceDF.transform(
          transformations.extractFromJson("person", "personData", personNameSchema)
        )

        val expectedDF = spark.createDF(
          List(
            (10, """{"name": "Bart cool", "age": 25}""", Row("Bart cool")),
            (20, """{"name": "Lisa frost", "age": 27}""", Row("Lisa frost"))
          ), List(
            ("id", IntegerType, true),
            ("person", StringType, true),
            ("personData", personNameSchema, true)
          )
        )

        assertSmallDataFrameEquality(actualDF, expectedDF)
      }

      'extractNonExistingSchema - {
        val wrongSchema = StructType(List(
          StructField("wrong_field", StringType)
        ))

        val actualDF = sourceDF.transform(
          transformations.extractFromJson("person", "personData", wrongSchema)
        )

        val expectedDF = spark.createDF(
          List(
            (10, """{"name": "Bart cool", "age": 25}""", Row(None.orNull)),
            (20, """{"name": "Lisa frost", "age": 27}""", Row(None.orNull))
          ), List(
            ("id", IntegerType, true),
            ("person", StringType, true),
            ("personData", wrongSchema, true)
          )
        )

        assertSmallDataFrameEquality(actualDF, expectedDF)
      }
    }

  }

}

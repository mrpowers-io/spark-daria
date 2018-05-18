package com.github.mrpowers.spark.daria.sql

import utest._
import SparkSessionExt._
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import DataFrameExt._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer

object DataFrameExtTest
    extends TestSuite
    with DataFrameComparer
    with SparkSessionTestWrapper {

  val tests = Tests {
    'printSchemaInCodeFormat - {

      "prints the schema in a code friendly format" - {

        val sourceDF = spark.createDF(
          List(
            ("jets", "football", 45),
            ("nacional", "soccer", 10)
          ), List(
            ("team", StringType, true),
            ("sport", StringType, true),
            ("goals_for", IntegerType, true)
          )
        )

        //      uncomment the next line if you want to check out the console output
        //      sourceDF.printSchemaInCodeFormat()

      }

    }

    'composeTransforms - {

      "runs a list of transforms" - {

        val sourceDF = spark.createDF(
          List(
            ("jets"),
            ("nacional")
          ), List(
            ("team", StringType, true)
          )
        )

        val transforms = List(
          ExampleTransforms.withGreeting()(_),
          ExampleTransforms.withCat("sandy")(_)
        )

        val actualDF = sourceDF.composeTransforms(transforms)

        val expectedDF = spark.createDF(
          List(
            ("jets", "hello world", "sandy meow"),
            ("nacional", "hello world", "sandy meow")
          ), List(
            ("team", StringType, true),
            ("greeting", StringType, false),
            ("cats", StringType, false)
          )
        )

        assertSmallDataFrameEquality(actualDF, expectedDF)

      }

    }

    'reorderColumns - {

      "reorders the columns in a DataFrame" - {

        val sourceDF = spark.createDF(
          List(
            ("jets", "hello", "sandy"),
            ("nacional", "hello", "sandy")
          ), List(
            ("team", StringType, true),
            ("greeting", StringType, false),
            ("cats", StringType, false)
          )
        )

        val actualDF = sourceDF.reorderColumns(
          Seq("greeting", "team", "cats")
        )

        val expectedDF = spark.createDF(
          List(
            ("hello", "jets", "sandy"),
            ("hello", "nacional", "sandy")
          ), List(
            ("greeting", StringType, false),
            ("team", StringType, true),
            ("cats", StringType, false)
          )
        )

        assertSmallDataFrameEquality(actualDF, expectedDF)

      }

      "matches the column ordering of one DataFrame with another DataFrame" - {

        val df1 = spark.createDF(
          List(
            ("jets", "hello", "sandy"),
            ("nacional", "hello", "sandy")
          ), List(
            ("team", StringType, true),
            ("greeting", StringType, false),
            ("cats", StringType, false)
          )
        )

        val df2 = spark.createDF(
          List(
            ("AAA", "BBB", "CCC")
          ), List(
            ("cats", StringType, false),
            ("greeting", StringType, false),
            ("team", StringType, true)
          )
        )

        val actualDF = df1.reorderColumns(
          df2.columns
        )

        val expectedDF = spark.createDF(
          List(
            ("sandy", "hello", "jets"),
            ("sandy", "hello", "nacional")
          ), List(
            ("cats", StringType, false),
            ("greeting", StringType, false),
            ("team", StringType, true)
          )
        )

        assertSmallDataFrameEquality(actualDF, expectedDF)

      }

    }

    'containsColumn - {

      "returns true if a DataFrame contains a column" - {

        val sourceDF = spark.createDF(
          List(
            ("jets"),
            ("nacional")
          ), List(
            ("team", StringType, true)
          )
        )

        assert(sourceDF.containsColumn("team") == true)
        assert(sourceDF.containsColumn("blah") == false)

      }

    }

    'columnDiff - {

      "returns the columns in otherDF that aren't in df" - {

        val sourceDF = spark.createDF(
          List(
            ("jets", "USA"),
            ("nacional", "Colombia")
          ), List(
            ("team", StringType, true),
            ("country", StringType, true)
          )
        )

        val otherDF = spark.createDF(
          List(
            ("jets"),
            ("nacional")
          ), List(
            ("team", StringType, true)
          )
        )

        val cols = sourceDF.columnDiff(otherDF)

        assert(cols == Seq("country"))

      }

    }

    'trans - {

      "works normally when the custom transformation appends the required columns" - {

        val sourceDF = spark.createDF(
          List(
            ("jets"),
            ("nacional")
          ), List(
            ("team", StringType, true)
          )
        )

        val ct = CustomTransform(
          transform = ExampleTransforms.withGreeting(),
          addedColumns = Seq("greeting")
        )

        val actualDF = sourceDF.trans(ct)

        val expectedDF = spark.createDF(
          List(
            ("jets", "hello world"),
            ("nacional", "hello world")
          ), List(
            ("team", StringType, true),
            ("greeting", StringType, false)
          )
        )

        assertSmallDataFrameEquality(actualDF, expectedDF)

      }

      "errors out if the column that's being added already exists" - {

        val sourceDF = spark.createDF(
          List(
            ("jets", "hi"),
            ("nacional", "hey")
          ), List(
            ("team", StringType, true),
            ("greeting", StringType, true)
          )
        )

        val ct = CustomTransform(
          transform = ExampleTransforms.withGreeting(),
          addedColumns = Seq("greeting")
        )

        val e = intercept[DataFrameColumnsException] {
          sourceDF.trans(ct)
        }

      }

      "errors out if the column that's being dropped doesn't exist" - {

        val sourceDF = spark.createDF(
          List(
            ("jets"),
            ("nacional")
          ), List(
            ("team", StringType, true)
          )
        )

        val ct = CustomTransform(
          transform = ExampleTransforms.withGreeting(),
          addedColumns = Seq("greeting"),
          removedColumns = Seq("foo")
        )

        val e = intercept[DataFrameColumnsException] {
          sourceDF.trans(ct)
        }

      }

      "errors out if the column isn't actually added" - {

        val sourceDF = spark.createDF(
          List(
            ("jets", "hi")
          ), List(
            ("team", StringType, true)
          )
        )

        val ct = CustomTransform(
          transform = ExampleTransforms.withCat("sandy"),
          addedColumns = Seq("greeting")
        )

        val e = intercept[DataFrameColumnsException] {
          sourceDF.trans(ct)
        }

      }

      "works when the required columns are included" - {

        val sourceDF = spark.createDF(
          List(
            ("jets"),
            ("nacional")
          ), List(
            ("team", StringType, true)
          )
        )

        val actualDF = sourceDF.trans(
          CustomTransform(
            transform = ExampleTransforms.withGreeting(),
            addedColumns = Seq("greeting"),
            requiredColumns = Seq("team")
          )
        )

        val expectedDF = spark.createDF(
          List(
            ("jets", "hello world"),
            ("nacional", "hello world")
          ), List(
            ("team", StringType, true),
            ("greeting", StringType, false)
          )
        )

        assertSmallDataFrameEquality(actualDF, expectedDF)

      }

      "errors out if required columns are missing" - {

        val sourceDF = spark.createDF(
          List(
            ("jets"),
            ("nacional")
          ), List(
            ("team", StringType, true)
          )
        )

        val e = intercept[MissingDataFrameColumnsException] {
          sourceDF.trans(
            CustomTransform(
              transform = ExampleTransforms.withGreeting(),
              addedColumns = Seq("greeting"),
              requiredColumns = Seq("something")
            )
          )
        }

      }

      "allows custom transformations to be chained" - {

        val sourceDF = spark.createDF(
          List(
            ("jets", "car")
          ), List(
            ("team", StringType, true),
            ("word", StringType, true)
          )
        )

        val actualDF = sourceDF
          .trans(
            CustomTransform(
              transform = ExampleTransforms.withGreeting(),
              addedColumns = Seq("greeting")
            )
          )
          .trans(
            CustomTransform(
              transform = ExampleTransforms.withCat("spanky"),
              addedColumns = Seq("cats")
            )
          )
          .trans(
            CustomTransform(
              transform = ExampleTransforms.dropWordCol(),
              removedColumns = Seq("word")
            )
          )

        val expectedDF = spark.createDF(
          List(
            ("jets", "hello world", "spanky meow")
          ), List(
            ("team", StringType, true),
            ("greeting", StringType, false),
            ("cats", StringType, false)
          )
        )

        assertSmallDataFrameEquality(actualDF, expectedDF)

      }

      "throws an exception if a column is not removed" - {

        val sourceDF = spark.createDF(
          List(
            ("jets", "hi")
          ), List(
            ("team", StringType, true),
            ("word", StringType, true)
          )
        )

        val ct = CustomTransform(
          transform = ExampleTransforms.withGreeting(),
          addedColumns = Seq("greeting"),
          removedColumns = Seq("word")
        )

        val e = intercept[DataFrameColumnsException] {
          sourceDF.trans(ct)
        }

      }

      "works if the columns that are removed are properly specified" - {

        val sourceDF = spark.createDF(
          List(
            ("jets", "hi")
          ), List(
            ("team", StringType, true),
            ("word", StringType, true)
          )
        )

        val ct = CustomTransform(
          transform = ExampleTransforms.dropWordCol(),
          removedColumns = Seq("word")
        )

        val actualDF = sourceDF.trans(ct)

        val expectedDF = spark.createDF(
          List(
            ("jets")
          ), List(
            ("team", StringType, true)
          )
        )

        assertSmallDataFrameEquality(actualDF, expectedDF)

      }

    }

    'flattenSchema - {

      "uses the StackOverflow answer format" - {

        val data = Seq(
          Row(Row("this", "is"), "something", "cool", ";)")
        )

        val schema = StructType(
          Seq(
            StructField(
              "foo",
              StructType(
                Seq(
                  StructField("bar", StringType, true),
                  StructField("baz", StringType, true)
                )
              ),
              true
            ),
            StructField("x", StringType, true),
            StructField("y", StringType, true),
            StructField("z", StringType, true)
          )
        )

        val df = spark.createDataFrame(
          spark.sparkContext.parallelize(data),
          StructType(schema)
        ).flattenSchema("_")

        val expectedDF = spark.createDF(
          List(
            ("this", "is", "something", "cool", ";)")
          ), List(
            ("foo_bar", StringType, true),
            ("foo_baz", StringType, true),
            ("x", StringType, true),
            ("y", StringType, true),
            ("z", StringType, true)
          )
        )

        assertSmallDataFrameEquality(df, expectedDF)

      }

      "flattens schema with the default delimiter" - {

        val data = Seq(
          Row("this", "is", Row("something", "cool"))
        )

        val schema = StructType(
          Seq(
            StructField("a", StringType, true),
            StructField("b", StringType, true),
            StructField(
              "c",
              StructType(
                Seq(
                  StructField("foo", StringType, true),
                  StructField("bar", StringType, true)
                )
              ),
              true
            )
          )
        )

        val df = spark.createDataFrame(
          spark.sparkContext.parallelize(data),
          StructType(schema)
        ).flattenSchema()

        val expectedDF = spark.createDF(
          List(
            ("this", "is", "something", "cool")
          ), List(
            ("a", StringType, true),
            ("b", StringType, true),
            ("c.foo", StringType, true),
            ("c.bar", StringType, true)
          )
        )

        assertSmallDataFrameEquality(df, expectedDF)

      }

      "flattens deeply nested schemas" - {

        val data = Seq(
          Row("this", "is", Row("something", "cool", Row("i", "promise")))
        )

        val schema = StructType(
          Seq(
            StructField("a", StringType, true),
            StructField("b", StringType, true),
            StructField(
              "c",
              StructType(
                Seq(
                  StructField("foo", StringType, true),
                  StructField("bar", StringType, true),
                  StructField(
                    "d",
                    StructType(
                      Seq(
                        StructField("crazy", StringType, true),
                        StructField("deep", StringType, true)
                      )
                    )
                  )
                )
              ),
              true
            )
          )
        )

        val df = spark.createDataFrame(
          spark.sparkContext.parallelize(data),
          StructType(schema)
        ).flattenSchema()

        val expectedDF = spark.createDF(
          List(
            ("this", "is", "something", "cool", "i", "promise")
          ), List(
            ("a", StringType, true),
            ("b", StringType, true),
            ("c.foo", StringType, true),
            ("c.bar", StringType, true),
            ("c.d.crazy", StringType, true),
            ("c.d.deep", StringType, true)
          )
        )

        assertSmallDataFrameEquality(df, expectedDF)

      }

      "allows for different column delimiters" - {

        val data = Seq(
          Row("this", "is", Row("something", "cool"))
        )

        val schema = StructType(
          Seq(
            StructField("a", StringType, true),
            StructField("b", StringType, true),
            StructField(
              "c",
              StructType(
                Seq(
                  StructField("foo", StringType, true),
                  StructField("bar", StringType, true)
                )
              ),
              true
            )
          )
        )

        val df = spark.createDataFrame(
          spark.sparkContext.parallelize(data),
          StructType(schema)
        ).flattenSchema(delimiter = "_")

        val expectedDF = spark.createDF(
          List(
            ("this", "is", "something", "cool")
          ), List(
            ("a", StringType, true),
            ("b", StringType, true),
            ("c_foo", StringType, true),
            ("c_bar", StringType, true)
          )
        )

        assertSmallDataFrameEquality(df, expectedDF)
      }

    }

  }

}

package com.github.mrpowers.spark.daria.sql

import utest._
import SparkSessionExt._
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{DataFrame, Row}
import DataFrameExt._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.functions._

object DataFrameExtTest extends TestSuite with DataFrameComparer with SparkSessionTestWrapper {

  val tests = Tests {

    'withColumnCast - {
      val sourceDF = spark.createDF(
        (3.14) :: Nil,
        ("value", DoubleType, true) :: Nil
      )
      val expectStringDF = spark.createDF(
        ("3.14") :: Nil,
        ("value", StringType, true) :: Nil
      )
      val expectIntDF = spark.createDF(
        (3) :: Nil,
        ("value", IntegerType, true) :: Nil
      )
      val expectBigIntDF = spark.createDF(
        (3L) :: Nil,
        ("value", LongType, true) :: Nil
      )
      "casts columns with a string specification" - {
        val asStringDF = sourceDF.withColumnCast(
          "value",
          "string"
        )
        assertSmallDataFrameEquality(
          asStringDF,
          expectStringDF
        )
        val asIntDF = sourceDF.withColumnCast(
          "value",
          "integer"
        )
        assertSmallDataFrameEquality(
          asIntDF,
          expectIntDF
        )
        val asBigIntDF = sourceDF.withColumnCast(
          "value",
          "bigint"
        )
        assertSmallDataFrameEquality(
          asBigIntDF,
          expectBigIntDF
        )
      }

      "casts columns with a datatype specification" - {
        val asStringDF = sourceDF.withColumnCast(
          "value",
          StringType
        )
        assertSmallDataFrameEquality(
          asStringDF,
          expectStringDF
        )
        val asIntDF = sourceDF.withColumnCast(
          "value",
          IntegerType
        )
        assertSmallDataFrameEquality(
          asIntDF,
          expectIntDF
        )
        val asBigIntDF = sourceDF.withColumnCast(
          "value",
          LongType
        )
        assertSmallDataFrameEquality(
          asBigIntDF,
          expectBigIntDF
        )
      }
    }

    'printSchemaInCodeFormat - {

      "prints the schema in a code friendly format" - {

        val sourceDF = spark.createDF(
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
        //      sourceDF.printSchemaInCodeFormat()

      }

    }

    'composeTransforms - {

      val sourceDF = spark.createDF(
        List(
          ("jets"),
          ("nacional")
        ),
        List(("team", StringType, true))
      )

      val expectedDF = spark.createDF(
        List(
          ("jets", "hello world", "sandy meow"),
          ("nacional", "hello world", "sandy meow")
        ),
        List(
          ("team", StringType, true),
          ("greeting", StringType, false),
          ("cats", StringType, false)
        )
      )

      "runs a list of transforms" - {

        val transforms = List(
          ExampleTransforms.withGreeting()(_),
          ExampleTransforms.withCat("sandy")(_)
        )

        val actualDF = sourceDF.composeTransforms(transforms)

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

      "runs a sequence of transforms" - {

        val actualDF =
          sourceDF.composeTransforms(
            ExampleTransforms.withGreeting(),
            ExampleTransforms.withCat("sandy")
          )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

    }

    'reorderColumns - {

      "reorders the columns in a DataFrame" - {

        val sourceDF = spark.createDF(
          List(
            ("jets", "hello", "sandy"),
            ("nacional", "hello", "sandy")
          ),
          List(
            ("team", StringType, true),
            ("greeting", StringType, false),
            ("cats", StringType, false)
          )
        )

        val actualDF = sourceDF.reorderColumns(
          Seq(
            "greeting",
            "team",
            "cats"
          )
        )

        val expectedDF = spark.createDF(
          List(
            ("hello", "jets", "sandy"),
            ("hello", "nacional", "sandy")
          ),
          List(
            ("greeting", StringType, false),
            ("team", StringType, true),
            ("cats", StringType, false)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

      "matches the column ordering of one DataFrame with another DataFrame" - {

        val df1 = spark.createDF(
          List(
            ("jets", "hello", "sandy"),
            ("nacional", "hello", "sandy")
          ),
          List(
            ("team", StringType, true),
            ("greeting", StringType, false),
            ("cats", StringType, false)
          )
        )

        val df2 = spark.createDF(
          List(("AAA", "BBB", "CCC")),
          List(
            ("cats", StringType, false),
            ("greeting", StringType, false),
            ("team", StringType, true)
          )
        )

        val actualDF = df1.reorderColumns(df2.columns)

        val expectedDF = spark.createDF(
          List(
            ("sandy", "hello", "jets"),
            ("sandy", "hello", "nacional")
          ),
          List(
            ("cats", StringType, false),
            ("greeting", StringType, false),
            ("team", StringType, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

    }

    'containsColumn - {

      "returns true if a DataFrame contains a column" - {

        val sourceDF = spark.createDF(
          List(
            ("jets"),
            ("nacional")
          ),
          List(("team", StringType, true))
        )

        assert(
          sourceDF.containsColumn("team") == true,
          sourceDF.containsColumn("blah") == false
        )

      }

      "returns false if a DataFrame contains a StructField" - {

        val sourceDF = spark.createDF(
          List(
            ("jets"),
            ("nacional")
          ),
          List(("team", StringType, true))
        )

        assert(
          sourceDF
            .containsColumn(
              StructField(
                "team",
                StringType,
                true
              )
            ) == true
        )
        assert(
          sourceDF
            .containsColumn(
              StructField(
                "blah",
                StringType,
                true
              )
            ) == false
        )

      }

    }

    'containsColumns - {

      "returns true if a DataFrame contains a column" - {

        val sourceDF =
          spark.createDF(
            List(
              ("jets", "blah"),
              ("nacional", "meow")
            ),
            List(
              ("team", StringType, true),
              ("sound", StringType, true)
            )
          )

        assert(
          sourceDF.containsColumns(
            "team",
            "sound"
          ) == true
        )
        assert(
          sourceDF.containsColumns(
            "team",
            "hi"
          ) == false
        )
        assert(
          sourceDF.containsColumns(
            "sound",
            "bye"
          ) == false
        )
        assert(sourceDF.containsColumns("blah") == false)

      }

    }

    'columnDiff - {

      "returns the columns in otherDF that aren't in df" - {

        val sourceDF =
          spark.createDF(
            List(
              ("jets", "USA"),
              ("nacional", "Colombia")
            ),
            List(
              ("team", StringType, true),
              ("country", StringType, true)
            )
          )

        val otherDF = spark.createDF(
          List(
            ("jets"),
            ("nacional")
          ),
          List(("team", StringType, true))
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
          ),
          List(("team", StringType, true))
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
          ),
          List(
            ("team", StringType, true),
            ("greeting", StringType, false)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

      "errors out if the column that's being added already exists" - {

        val sourceDF = spark.createDF(
          List(
            ("jets", "hi"),
            ("nacional", "hey")
          ),
          List(
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
          ),
          List(("team", StringType, true))
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

        val sourceDF =
          spark.createDF(
            List(("jets", "hi")),
            List(("team", StringType, true))
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
          ),
          List(("team", StringType, true))
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
          ),
          List(
            ("team", StringType, true),
            ("greeting", StringType, false)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

      "errors out if required columns are missing" - {

        val sourceDF = spark.createDF(
          List(
            ("jets"),
            ("nacional")
          ),
          List(("team", StringType, true))
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
          List(("jets", "car")),
          List(
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

        val expectedDF =
          spark.createDF(
            List(("jets", "hello world", "spanky meow")),
            List(
              ("team", StringType, true),
              ("greeting", StringType, false),
              ("cats", StringType, false)
            )
          )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

      "throws an exception if a column is not removed" - {

        val sourceDF = spark.createDF(
          List(("jets", "hi")),
          List(
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
          List(("jets", "hi")),
          List(
            ("team", StringType, true),
            ("word", StringType, true)
          )
        )

        val ct = CustomTransform(
          transform = ExampleTransforms.dropWordCol(),
          removedColumns = Seq("word")
        )

        val actualDF = sourceDF.trans(ct)

        val expectedDF =
          spark.createDF(
            List(("jets")),
            List(("team", StringType, true))
          )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

    }

    'flattenSchema - {

      "uses the StackOverflow answer format" - {

        val data = Seq(
          Row(
            Row(
              Row("this"),
              "is"
            ),
            "something",
            "cool",
            ";)"
          )
        )

        val schema = StructType(
          Seq(
            StructField(
              "foo",
              StructType(
                Seq(
                  StructField(
                    "bar",
                    StructType(
                      Seq(
                        StructField(
                          "zoo",
                          StringType,
                          true
                        )
                      )
                    )
                  ),
                  StructField(
                    "baz",
                    StringType,
                    true
                  )
                )
              ),
              true
            ),
            StructField(
              "x",
              StringType,
              true
            ),
            StructField(
              "y",
              StringType,
              true
            ),
            StructField(
              "z",
              StringType,
              true
            )
          )
        )

        val df = spark
          .createDataFrame(
            spark.sparkContext.parallelize(data),
            StructType(schema)
          )
          .flattenSchema("_")

        val expectedDF = spark.createDF(
          List(("this", "is", "something", "cool", ";)")),
          List(
            ("foo_bar_zoo", StringType, true),
            ("foo_baz", StringType, true),
            ("x", StringType, true),
            ("y", StringType, true),
            ("z", StringType, true)
          )
        )

        assertSmallDataFrameEquality(
          df,
          expectedDF
        )

      }

      "flattens schema with the default delimiter" - {

        val data = Seq(
          Row(
            "this",
            "is",
            Row(
              "something",
              "cool"
            )
          )
        )

        val schema = StructType(
          Seq(
            StructField(
              "a",
              StringType,
              true
            ),
            StructField(
              "b",
              StringType,
              true
            ),
            StructField(
              "c",
              StructType(
                Seq(
                  StructField(
                    "foo",
                    StringType,
                    true
                  ),
                  StructField(
                    "bar",
                    StringType,
                    true
                  )
                )
              ),
              true
            )
          )
        )

        val df = spark
          .createDataFrame(
            spark.sparkContext.parallelize(data),
            StructType(schema)
          )
          .flattenSchema()

        val expectedDF =
          spark.createDF(
            List(("this", "is", "something", "cool")),
            List(
              ("a", StringType, true),
              ("b", StringType, true),
              ("c.foo", StringType, true),
              ("c.bar", StringType, true)
            )
          )

        assertSmallDataFrameEquality(
          df,
          expectedDF
        )

      }

      "flattens deeply nested schemas" - {

        val data =
          Seq(
            Row(
              "this",
              "is",
              Row(
                "something",
                "cool",
                Row(
                  "i",
                  "promise"
                )
              )
            )
          )

        val schema = StructType(
          Seq(
            StructField(
              "a",
              StringType,
              true
            ),
            StructField(
              "b",
              StringType,
              true
            ),
            StructField(
              "c",
              StructType(
                Seq(
                  StructField(
                    "foo",
                    StringType,
                    true
                  ),
                  StructField(
                    "bar",
                    StringType,
                    true
                  ),
                  StructField(
                    "d",
                    StructType(
                      Seq(
                        StructField(
                          "crazy",
                          StringType,
                          true
                        ),
                        StructField(
                          "deep",
                          StringType,
                          true
                        )
                      )
                    )
                  )
                )
              ),
              true
            )
          )
        )

        val df = spark
          .createDataFrame(
            spark.sparkContext.parallelize(data),
            StructType(schema)
          )
          .flattenSchema()

        val expectedDF = spark.createDF(
          List(("this", "is", "something", "cool", "i", "promise")),
          List(
            ("a", StringType, true),
            ("b", StringType, true),
            ("c.foo", StringType, true),
            ("c.bar", StringType, true),
            ("c.d.crazy", StringType, true),
            ("c.d.deep", StringType, true)
          )
        )

        assertSmallDataFrameEquality(
          df,
          expectedDF
        )

      }

      "allows for different column delimiters" - {

        val data = Seq(
          Row(
            "this",
            "is",
            Row(
              "something",
              "cool"
            )
          )
        )

        val schema = StructType(
          Seq(
            StructField(
              "a",
              StringType,
              true
            ),
            StructField(
              "b",
              StringType,
              true
            ),
            StructField(
              "c",
              StructType(
                Seq(
                  StructField(
                    "foo",
                    StringType,
                    true
                  ),
                  StructField(
                    "bar",
                    StringType,
                    true
                  )
                )
              ),
              true
            )
          )
        )

        val df = spark
          .createDataFrame(
            spark.sparkContext.parallelize(data),
            StructType(schema)
          )
          .flattenSchema(delimiter = "_")

        val expectedDF =
          spark.createDF(
            List(("this", "is", "something", "cool")),
            List(
              ("a", StringType, true),
              ("b", StringType, true),
              ("c_foo", StringType, true),
              ("c_bar", StringType, true)
            )
          )

        assertSmallDataFrameEquality(
          df,
          expectedDF
        )
      }

    }

    'selectSortedCols - {

      "select col with all field sorted" - {

        val data = Seq(
          Row(
            Row(
              "bayVal",
              "baxVal",
              Row("this", "yoVal"),
              "is"
            ),
            Seq(
              Row("yVal", "xVal"),
              Row("yVal1", "xVal1")
            ),
            "something",
            "cool",
            ";)"
          )
        )

        val schema = StructType(
          Seq(
            StructField(
              "foo",
              StructType(
                Seq(
                  StructField(
                    "bay",
                    StringType,
                    true
                  ),
                  StructField(
                    "bax",
                    StringType,
                    true
                  ),
                  StructField(
                    "bar",
                    StructType(
                      Seq(
                        StructField(
                          "zoo",
                          StringType,
                          true
                        ),
                        StructField(
                          "yoo",
                          StringType,
                          true
                        )
                      )
                    )
                  ),
                  StructField(
                    "baz",
                    StringType,
                    true
                  ),
                )
              ),
              true
            ),
            StructField(
              "w",
              ArrayType(StructType(Seq(StructField("y", StringType, true), StructField("x", StringType, true)))),
              true
            ),
            StructField(
              "x",
              StringType,
              true
            ),
            StructField(
              "y",
              StringType,
              true
            ),
            StructField(
              "z",
              StringType,
              true
            )
          )
        )

        val df = spark
          .createDataFrame(
            spark.sparkContext.parallelize(data),
            StructType(schema)
          )
          .selectSortedCols

        val expectedData = Seq(
          Row(
            Row(
              Row("yoVal", "this"),
              "baxVal",
              "bayVal",
              "is"
            ),
            Seq(
              Row("xVal", "yVal"),
              Row("xVal1", "yVal1")
            ),
            "something",
            "cool",
            ";)"
          )
        )

        val expectedSchema = StructType(
          Seq(
            StructField(
              "foo",
              StructType(
                Seq(
                  StructField(
                    "bar",
                    StructType(
                      Seq(
                        StructField(
                          "yoo",
                          StringType,
                          true
                        ),
                        StructField(
                          "zoo",
                          StringType,
                          true
                        )
                      )
                    ),
                    false
                  ),
                  StructField(
                    "bax",
                    StringType,
                    true
                  ),
                  StructField(
                    "bay",
                    StringType,
                    true
                  ),
                  StructField(
                    "baz",
                    StringType,
                    true
                  )
                )
              ),
              false
            ),
            StructField(
              "w",
              ArrayType(StructType(Seq(StructField("x", StringType, true), StructField("y", StringType, true))), false),
              true
            ),
            StructField(
              "x",
              StringType,
              true
            ),
            StructField(
              "y",
              StringType,
              true
            ),
            StructField(
              "z",
              StringType,
              true
            )
          )
        )

        val expectedDF = spark
          .createDataFrame(
            spark.sparkContext.parallelize(expectedData),
            StructType(expectedSchema)
          )

        assertSmallDataFrameEquality(
          df,
          expectedDF,
          ignoreNullable = true
        )
      }

    }

    'structureSchema - {
      "structure schema with default delimiter" - {
        val data = Seq(
          Row(
            ";)",
            Row(
              "is",
              Row("this")
            )
          )
        )

        val schema = StructType(
          Seq(
            StructField(
              "z",
              StringType,
              true
            ),
            StructField(
              "foo",
              StructType(
                Seq(
                  StructField(
                    "baz",
                    StringType,
                    true
                  ),
                  StructField(
                    "bar",
                    StructType(
                      Seq(
                        StructField(
                          "zoo",
                          StringType,
                          true
                        )
                      )
                    )
                  )
                )
              ),
              true
            )
          )
        )

        val df = spark
          .createDataFrame(
            spark.sparkContext.parallelize(data),
            StructType(schema)
          )

        df.printSchema()
        val delimiter = "_"
        df.flattenSchema(delimiter).printSchema()
        val expectedDF = df
          .flattenSchema(delimiter)
          .structureSchema(delimiter)
          .setNullableForAllColumns(true) //for some reason spark changes nullability of struct columns
        expectedDF.printSchema()

        DariaValidator.validateSchema(
          expectedDF,
          schema
        )
      }
    }

    'dropNestedColumn - {
      "drop nested column" - {
        val data = Seq(
          Row(
            ";)",
            Row(
              "is",
              Row("this")
            )
          )
        )

        val schema = StructType(
          Seq(
            StructField(
              "z",
              StringType,
              true
            ),
            StructField(
              "foo",
              StructType(
                Seq(
                  StructField(
                    "baz",
                    StringType,
                    true
                  ),
                  StructField(
                    "bar",
                    StructType(
                      Seq(
                        StructField(
                          "zoo",
                          StringType,
                          true
                        )
                      )
                    )
                  )
                )
              ),
              true
            )
          )
        )

        val df = spark
          .createDataFrame(
            spark.sparkContext.parallelize(data),
            StructType(schema)
          )

        df.printSchema()

        val expectedSchema = StructType(
          Seq(
            StructField(
              "z",
              StringType,
              true
            ),
            StructField(
              "foo",
              StructType(
                Seq(
                  StructField(
                    "bar",
                    StructType(
                      Seq(
                        StructField(
                          "zoo",
                          StringType,
                          true
                        )
                      )
                    )
                  )
                )
              ),
              true
            )
          )
        )

        val expectedDf = df.dropNestedColumn("foo.baz").setNullableForAllColumns(true)
        expectedDf.printSchema()

        DariaValidator.validateSchema(
          expectedDf,
          expectedSchema
        )

      }
    }

    'composeTrans - {

      def withCountry()(df: DataFrame): DataFrame = {
        df.withColumn(
          "country",
          when(
            col("city") === "Calgary",
            "Canada"
          ).when(
            col("city") === "Buenos Aires",
            "Argentina"
          ).when(
            col("city") === "Cape Town",
            "South Africa"
          )
        )
      }

      def withHemisphere()(df: DataFrame): DataFrame = {
        df.withColumn(
          "hemisphere",
          when(
            col("country") === "Canada",
            "Northern Hemisphere"
          ).when(
            col("country") === "Argentina",
            "Southern Hemisphere"
          ).when(
            col("country") === "South Africa",
            "Southern Hemisphere"
          )
        )
      }

      "executes a series of CustomTransforms" - {

        val df =
          spark.createDF(
            List(
              ("Calgary"),
              ("Buenos Aires"),
              ("Cape Town")
            ),
            List(("city", StringType, true))
          )

        val countryCT = CustomTransform(
          transform = withCountry(),
          requiredColumns = Seq("city"),
          addedColumns = Seq("country"),
          skipWhenPossible = false
        )

        val hemisphereCT = CustomTransform(
          transform = withHemisphere(),
          requiredColumns = Seq("country"),
          addedColumns = Seq("hemisphere"),
          skipWhenPossible = false
        )

        val actualDF = df.composeTrans(
          List(
            countryCT,
            hemisphereCT
          )
        )

        val expectedDF = spark.createDF(
          List(
            ("Calgary", "Canada", "Northern Hemisphere"),
            ("Buenos Aires", "Argentina", "Southern Hemisphere"),
            ("Cape Town", "South Africa", "Southern Hemisphere")
          ),
          List(
            ("city", StringType, true),
            ("country", StringType, true),
            ("hemisphere", StringType, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

      "runs CustomTransformations, but only when necessary" - {

        val df = spark.createDF(
          List(
            ("Calgary", "Canada"),
            ("Buenos Aires", "Argentina"),
            ("Cape Town", "South Africa")
          ),
          List(
            ("city", StringType, true),
            ("country", StringType, true)
          )
        )

        val countryCT = CustomTransform(
          transform = withCountry(),
          requiredColumns = Seq("city"),
          addedColumns = Seq("country"),
          skipWhenPossible = true
        )

        val hemisphereCT = CustomTransform(
          transform = withHemisphere(),
          requiredColumns = Seq("country"),
          addedColumns = Seq("hemisphere"),
          skipWhenPossible = true
        )

        val actualDF = df.composeTrans(
          List(
            countryCT,
            hemisphereCT
          )
        )

        // examine the explain output to verify that the countryCT transformation isn't executed
        //        actualDF.explain()

        val expectedDF = spark.createDF(
          List(
            ("Calgary", "Canada", "Northern Hemisphere"),
            ("Buenos Aires", "Argentina", "Southern Hemisphere"),
            ("Cape Town", "South Africa", "Southern Hemisphere")
          ),
          List(
            ("city", StringType, true),
            ("country", StringType, true),
            ("hemisphere", StringType, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

    }

    'killDuplicates - {

      "removes any rows that have a duplicate" - {

        val df = spark
          .createDF(
            List(
              ("a", "b", 1),
              ("a", "b", 2),
              ("a", "b", 3),
              ("z", "b", 4),
              ("a", "x", 5)
            ),
            List(
              ("letter1", StringType, true),
              ("letter2", StringType, true),
              ("number1", IntegerType, true)
            )
          )
          .killDuplicates(
            col("letter1"),
            col("letter2")
          )

        val expectedDF = spark.createDF(
          List(
            ("z", "b", 4),
            ("a", "x", 5)
          ),
          List(
            ("letter1", StringType, true),
            ("letter2", StringType, true),
            ("number1", IntegerType, true)
          )
        )

        assertSmallDataFrameEquality(
          df,
          expectedDF,
          orderedComparison = false
        )

      }

      "removes any rows that have a duplicate ()" - {

        val df = spark
          .createDF(
            List(
              ("a", "b", 1),
              ("a", "b", 1),
              ("a", "b", 3),
              ("z", "b", 4),
              ("a", "x", 5)
            ),
            List(
              ("letter1", StringType, true),
              ("letter2", StringType, true),
              ("number1", IntegerType, true)
            )
          )
          .killDuplicates()

        val expectedDF = spark.createDF(
          List(
            ("a", "b", 3),
            ("z", "b", 4),
            ("a", "x", 5)
          ),
          List(
            ("letter1", StringType, true),
            ("letter2", StringType, true),
            ("number1", IntegerType, true)
          )
        )

        assertSmallDataFrameEquality(
          df,
          expectedDF,
          orderedComparison = false
        )

      }

      "removes any rows that have a duplicate (str)" - {

        val df = spark
          .createDF(
            List(
              ("a", "b", 1),
              ("a", "b", 2),
              ("a", "b", 3),
              ("z", "b", 4),
              ("a", "x", 5)
            ),
            List(
              ("letter1", StringType, true),
              ("letter2", StringType, true),
              ("number1", IntegerType, true)
            )
          )
          .killDuplicates(
            "letter1",
            "letter2"
          )

        val expectedDF = spark.createDF(
          List(
            ("z", "b", 4),
            ("a", "x", 5)
          ),
          List(
            ("letter1", StringType, true),
            ("letter2", StringType, true),
            ("number1", IntegerType, true)
          )
        )

        assertSmallDataFrameEquality(
          df,
          expectedDF,
          orderedComparison = false
        )

      }

      "works with a single column too" - {

        val df = spark
          .createDF(
            List(
              ("a", "b", 1),
              ("a", "b", 2),
              ("a", "b", 3),
              ("z", "b", 4),
              ("a", "x", 5)
            ),
            List(
              ("letter1", StringType, true),
              ("letter2", StringType, true),
              ("number1", IntegerType, true)
            )
          )
          .killDuplicates(col("letter1"))

        val expectedDF = spark.createDF(
          List(("z", "b", 4)),
          List(
            ("letter1", StringType, true),
            ("letter2", StringType, true),
            ("number1", IntegerType, true)
          )
        )

        assertSmallDataFrameEquality(
          df,
          expectedDF,
          orderedComparison = false
        )

      }

    }

    'renameColumns - {

      "change dash or space to underscore" - {

        val df = spark
          .createDF(
            List(
              ("John", 1),
              ("Paul", 2),
              ("Jane", 3)
            ),
            List(
              ("user-name", StringType, true),
              ("user id", IntegerType, true)
            )
          )
          .renameColumns(
            _.trim.replaceAll(
              "[\\s-]+",
              "_"
            )
          )

        val expectedDF = spark
          .createDF(
            List(
              ("John", 1),
              ("Paul", 2),
              ("Jane", 3)
            ),
            List(
              ("user_name", StringType, true),
              ("user_id", IntegerType, true)
            )
          )
        assert(df.columns.toSet == expectedDF.columns.toSet)
      }

      "makes multiple changes" - {

        val df = spark
          .createDF(
            List(
              ("foo", "bar", "car")
            ),
            List(
              ("SomeColumn", StringType, true),
              ("Another Column", StringType, true),
              ("BAR_COLUMN", StringType, true)
            )
          )
          .renameColumns(
            _.trim
              .replaceAll(
                "[\\s-]+",
                "_"
              )
              .replaceAll(
                "([A-Z]+)([A-Z][a-z])",
                "$1_$2"
              )
              .replaceAll(
                "([a-z\\d])([A-Z])",
                "$1_$2"
              )
              .toLowerCase
          )

        df.columns.toList ==> Seq(
          "some_column",
          "another_column",
          "bar_column"
        )

      }

    }

    'dropColumns - {

      "drop columns which start with underscore" - {

        val df = spark
          .createDF(
            List(
              ("John", 1, 101),
              ("Paul", 2, 102),
              ("Jane", 3, 103)
            ),
            List(
              ("name", StringType, true),
              ("id", IntegerType, true),
              ("_internal_id", IntegerType, true)
            )
          )
          .dropColumns(_.startsWith("_"))

        val expectedDF = spark
          .createDF(
            List(
              ("John", 1),
              ("Paul", 2),
              ("Jane", 3)
            ),
            List(
              ("name", StringType, true),
              ("id", IntegerType, true)
            )
          )
        assert(df.columns.toSet == expectedDF.columns.toSet)
        assertSmallDataFrameEquality(
          df,
          expectedDF,
          orderedComparison = false
        )

      }

      "setNullableForAllColumn" - {
        val df = spark
          .createDataFrame(
            spark.sparkContext.parallelize(
              Seq(
                Row(
                  1977,
                  List(
                    Row(
                      "John",
                      1
                    )
                  )
                ),
                Row(
                  1987,
                  List(
                    Row(
                      "Paul",
                      2
                    )
                  )
                ),
                Row(
                  1983,
                  List(
                    Row(
                      "Jack",
                      3
                    )
                  )
                )
              )
            ),
            new StructType()
              .add(
                "year",
                IntegerType,
                false
              )
              .add(
                "person",
                ArrayType(
                  new StructType()
                    .add(
                      "name",
                      StringType,
                      false
                    )
                    .add(
                      "id",
                      IntegerType,
                      false
                    )
                )
              )
          )
          .setNullableForAllColumns(true)

        val expectedDF = spark.createDataFrame(
          spark.sparkContext.parallelize(
            Seq(
              Row(
                1977,
                List(
                  Row(
                    "John",
                    1
                  )
                )
              ),
              Row(
                1987,
                List(
                  Row(
                    "Paul",
                    2
                  )
                )
              ),
              Row(
                1983,
                List(
                  Row(
                    "Jack",
                    3
                  )
                )
              )
            )
          ),
          new StructType()
            .add(
              "year",
              IntegerType,
              true
            )
            .add(
              "person",
              ArrayType(
                new StructType()
                  .add(
                    "name",
                    StringType,
                    true
                  )
                  .add(
                    "id",
                    IntegerType,
                    true
                  )
              )
            )
        )
        assertSmallDataFrameEquality(
          df,
          expectedDF,
          orderedComparison = true
        )
      }

    }

  }

}

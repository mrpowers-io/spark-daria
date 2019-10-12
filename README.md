# spark-daria

Spark helper methods to maximize developer productivity.

[![Build Status](https://travis-ci.org/MrPowers/spark-daria.svg?branch=master)](https://travis-ci.org/MrPowers/spark-daria) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/cf2c0624682b4487a3b3e5c8330f1fbe)](https://www.codacy.com/app/MrPowers/spark-daria?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=MrPowers/spark-daria&amp;utm_campaign=Badge_Grade) [![Maintainability](https://api.codeclimate.com/v1/badges/513fcd36d6be35191737/maintainability)](https://codeclimate.com/github/MrPowers/spark-daria/maintainability)
[![Gitter](https://badges.gitter.im/spark-daria/community.svg)](https://gitter.im/spark-daria/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

![typical daria](https://github.com/MrPowers/spark-daria/blob/master/daria.png)

## Setup

**Option 1: Maven**

Fetch the JAR file from Maven.

```scala
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

// Scala 2.11
libraryDependencies += "mrpowers" % "spark-daria" % "0.35.0-s_2.11"

// Scala 2.12, Spark 2.4+
libraryDependencies += "mrpowers" % "spark-daria" % "0.35.0-s_2.12"
```

**Option 2: JitPack**

Update your `build.sbt` file as follows.

```scala
resolvers += "jitpack" at "https://jitpack.io"
libraryDependencies += "com.github.mrpowers" % "spark-daria" % "v0.26.0"
```
**Accessing spark-daria versions for different Spark versions**

Different spark-daria versions are compatible with different Spark versions.  In general, the latest spark-daria versions are always compatible with the latest Spark versions.

|       | 0.35.0             | 0.34.0             | 0.33.2             |
|-------|--------------------|--------------------|--------------------|
| 2.0.0 | :x:                | :x:                | :x:                |
| 2.1.0 | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| 2.2.2 | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| 2.3.0 | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| 2.3.1 | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| 2.4.0 | :white_check_mark: | :white_check_mark: | :white_check_mark: |

Email me if you need a custom spark-daria version and I'll help you out :wink:

## PySpark

Use [quinn](https://github.com/MrPowers/quinn) to access all these same functions in PySpark.

## Usage

spark-daria provides different types of functions that will make your life as a Spark developer easier:

1. Core extensions
2. Column functions / UDFs
3. Custom transformations
4. Helper methods
5. DataFrame validators

The following overview will give you an idea of the types of functions that are provided by spark-daria, but you'll need to dig into the docs to learn about all the methods.

### Core extensions

The core extensions add methods to existing Spark classes that will help you write beautiful code.

The native Spark API forces you to write code like this.

```scala
col("is_nice_person").isNull && col("likes_peanut_butter") === false
```

When you import the spark-daria `ColumnExt` class, you can write idiomatic Scala code like this:

```scala
import com.github.mrpowers.spark.daria.sql.ColumnExt._

col("is_nice_person").isNull && col("likes_peanut_butter").isFalse
```

[This blog post](https://medium.com/@mrpowers/manually-creating-spark-dataframes-b14dae906393) describes how to use the spark-daria `createDF()` method that's much better than the `toDF()` and `createDataFrame()` methods provided by Spark.

See the `ColumnExt`, `DataFrameExt`, and `SparkSessionExt` objects for all the core extensions offered by spark-daria.

### Column functions

Column functions can be used in addition to the [org.apache.spark.sql.functions](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$).

Here is how to remove all whitespace from a string with the native Spark API:

```scala
import org.apache.spark.sql.functions._

regexp_replace(col("first_name"), "\\s+", "")
```

The spark-daria `removeAllWhitespace()` function lets you express this logic with code that's more readable.

```scala
import com.github.mrpowers.spark.daria.sql.functions._

removeAllWhitespace(col("first_name"))
```

### Custom transformations

Custom transformations have the following method signature so they can be passed as arguments to the Spark `DataFrame#transform()` method.

```scala
def someCustomTransformation(arg1: String)(df: DataFrame): DataFrame = {
  // code that returns a DataFrame
}
```

The spark-daria `snakeCaseColumns()` custom transformation snake_cases all of the column names in a DataFrame.

```scala
import com.github.mrpowers.spark.daria.sql.transformations._

val betterDF = df.transform(snakeCaseColumns())
```

Protip: You'll always want to deal with snake_case column names in Spark - use this function if your column names contain spaces of uppercase letters.

### Helper methods

The DataFrame helper methods make it easy to convert DataFrame columns into Arrays or Maps.  Here's how to convert a column to an Array.

```scala
import com.github.mrpowers.spark.daria.sql.DataFrameHelpers._

val arr = DataFrameHelpers.columnToArray[Int](sourceDF, "num")
```

### DataFrame validators

DataFrame validators check that DataFrames contain certain columns or a specific schema.  They throw descriptive error messages if the DataFrame schema is not as expected.  DataFrame validators are a great way to make sure your application gives descriptive error messages.

Let's look at a method that makes sure a DataFrame contains the expected columns.

```scala
val sourceDF = Seq(
  ("jets", "football"),
  ("nacional", "soccer")
).toDF("team", "sport")

val requiredColNames = Seq("team", "sport", "country", "city")

validatePresenceOfColumns(sourceDF, requiredColNames)

// throws this error message: com.github.mrpowers.spark.daria.sql.MissingDataFrameColumnsException: The [country, city] columns are not included in the DataFrame with the following columns [team, sport]
```

## Documentation

[Here is the latest spark-daria documentation](https://mrpowers.github.io/docs/spark_daria/latest/index.html).

Studying these docs will make you a better Spark developer!

## :two_women_holding_hands: :two_men_holding_hands: :couple: Contribution Criteria

We are actively looking for contributors to add functionality that fills in the gaps of the Spark source code.

To get started, fork the project and submit a pull request.  Please write tests!

After submitting a couple of good pull requests, you'll be added as a contributor to the project.

Continued excellence will be rewarded with push access to the master branch.

## Publishing

Build the JAR / POM files with `sbt +spDist` as described in [this GitHub issue](https://github.com/databricks/sbt-spark-package/issues/18#issuecomment-184107369).

Manually upload the zip files to [Spark Packages](https://spark-packages.org/).

Make a GitHub release so the code is available via JitPack.

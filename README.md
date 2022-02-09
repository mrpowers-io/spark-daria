# spark-daria

Spark helper methods to maximize developer productivity.

CI: [![GitHub Build Status](https://github.com/MrPowers/spark-daria/actions/workflows/ci.yml/badge.svg)](https://github.com/MrPowers/spark-daria/actions/workflows/ci.yml)

Code quality: [![Maintainability](https://api.codeclimate.com/v1/badges/513fcd36d6be35191737/maintainability)](https://codeclimate.com/github/MrPowers/spark-daria/maintainability)

![typical daria](https://github.com/MrPowers/spark-daria/blob/main/daria.png)

## Setup

Fetch the JAR file from Maven.

```scala
// Spark 3
libraryDependencies += "com.github.mrpowers" %% "spark-daria" % "1.2.3"

// Spark 2
libraryDependencies += "com.github.mrpowers" %% "spark-daria" % "0.39.0"
```

You can find the spark-daria releases for different Scala versions:

* [Scala 2.11 versions here](https://repo1.maven.org/maven2/com/github/mrpowers/spark-daria_2.11/)
* [Scala 2.12 versions here](https://repo1.maven.org/maven2/com/github/mrpowers/spark-daria_2.12/)
* [Scala 2.13 versions here](https://repo1.maven.org/maven2/com/github/mrpowers/spark-daria_2.13/)
* [legacy versions are here](https://mvnrepository.com/artifact/mrpowers/spark-daria?repo=spark-packages).

## Writing Beautiful Spark Code

Reading [Beautiful Spark Code](https://leanpub.com/beautiful-spark/) is the best way to learn how to build Spark projects and leverage spark-daria.

spark-daria will make you a more productive Spark programmer.  Studying the spark-daria codebase will help you understand how to organize Spark codebases.

## PySpark

Use [quinn](https://github.com/MrPowers/quinn) to access similar functions in PySpark.

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

#### Datetime functions

* `beginningOfWeek`
* `endOfWeek`
* `beginningOfMonth`
* `endOfMonth`

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

val arr = columnToArray[Int](sourceDF, "num")
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

[Here is the latest spark-daria documentation](https://mrpowers.github.io/spark-daria/latest/api/).

Studying these docs will make you a better Spark developer!

## :two_women_holding_hands: :two_men_holding_hands: :couple: Contribution Criteria

We are actively looking for contributors to add functionality that fills in the gaps of the Spark source code.

To get started, fork the project and submit a pull request.  Please write tests!

After submitting a couple of good pull requests, you'll be added as a contributor to the project.

## Publishing

Sonatype passwords can go stale and need to be reset periodically.  Go to the Sonatype website and log in to make sure your password is working to avoid errors that are difficult to understand and debug.

You need GPG installed on your machine as well.  You can install it with `brew install gnupg`.

You need to get GPG keys properly setup on every machine.  You can follow [these instructions](https://github.com/sbt/sbt-ci-release#gpg) to get your GPG key setup on each machine.

1. Version bump commit and create GitHub tag

2. Publish documentation with `sbt ghpagesPushSite`

3. Publish JAR

Run `sbt` to open the SBT console.

*IMPORTANT* Run `sbt clean` before running the publish commands!  Otherwise you may run into [this error](https://github.com/xerial/sbt-sonatype/issues/282).

Run `> ; + publishSigned; sonatypeBundleRelease` to create the JAR files and release them to Maven.  These commands are made available by the [sbt-sonatype](https://github.com/xerial/sbt-sonatype) plugin.

When the release command is run, you'll be prompted to enter your GPG passphrase.

The Sonatype credentials should be stored in the `~/.sbt/sonatype_credentials` file in this format:

```
realm=Sonatype Nexus Repository Manager
host=oss.sonatype.org
user=$USERNAME
password=$PASSWORD
```

New Sonatype accounts need a different host, as [described here](https://github.com/xerial/sbt-sonatype/issues/224#issuecomment-812601799).  My Sonatype account was created before February 2021, so this does not apply to me.

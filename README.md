# spark-daria

Spark helper methods to maximize developer productivity.

[![Build Status](https://travis-ci.org/MrPowers/spark-daria.svg?branch=master)](https://travis-ci.org/MrPowers/spark-daria)

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/cf2c0624682b4487a3b3e5c8330f1fbe)](https://www.codacy.com/app/MrPowers/spark-daria?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=MrPowers/spark-daria&amp;utm_campaign=Badge_Grade)

![typical daria](https://github.com/MrPowers/spark-daria/blob/master/daria.png)

## Setup

**Option 1: Maven**

Fetch the JAR file from Maven.

```scala
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies += "mrpowers" % "spark-daria" % "2.2.0_0.12.0"
```

**Option 2: Spark Packages**

1. Add the [sbt-spark-package plugin](https://github.com/databricks/sbt-spark-package) to your application.  The spark-daria releases are maintained in [Spark Packages](https://spark-packages.org/package/mrpowers/spark-daria).

2. Update your build.sbt file: `spDependencies += "mrpowers/spark-daria:2.2.0_0.12.0"`

**Option 3: JitPack**

Update your `build.sbt` file as follows.

```scala
resolvers += "jitpack" at "https://jitpack.io"
libraryDependencies += "com.github.mrpowers" % "spark-daria" % "v2.2.0_0.17.1"
```
**Accessing spark-daria versions for different Spark versions**

Message me if you need spark-daria to be compiled with a different Spark version and I'll help you out :wink:

## :two_women_holding_hands: :two_men_holding_hands: :couple: Contribution Criteria

We are actively looking for contributors to add functionality that fills in the gaps of the Spark source code.

To get started, fork the project and submit a pull request.  Please write tests!

After submitting a couple of good pull requests, you'll be added as a contributor to the project.

Continued excellence will be rewarded with push access to the master branch.


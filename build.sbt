import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

SbtScalariform.scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(DoubleIndentClassDeclaration, true)
  .setPreference(SpacesAroundMultiImports, false)

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

organization := "mrpowers"
name := "spark-daria"

version := "0.26.1"

crossScalaVersions := Seq("2.11.12", "2.12.7")
scalaVersion := "2.11.12"
sparkVersion := "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.0" % "provided"

spName := "mrpowers/spark-daria"

spShortDescription := "Spark helper methods to maximize developer productivity"

spDescription := "DataFrame validations, Column extensions, SparkSession extensions, sql functions, DataFrame transformations, and DataFrameHelpers."

//libraryDependencies += "MrPowers" % "spark-fast-tests" % "0.17.0" % "test"

libraryDependencies += "com.lihaoyi" %% "utest" % "0.6.3" % "test"
testFrameworks += new TestFramework("com.github.mrpowers.spark.daria.CustomFramework")

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "-" + module.revision + "." + artifact.extension
}

// All Spark Packages need a license
licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

fork in Test := true

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled","-Duser.timezone=GMT")

/* Only invoked when you do `doc` in SBT */
scalacOptions in (Compile, doc) += "-groups"

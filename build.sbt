import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

SbtScalariform.scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(DoubleIndentClassDeclaration, true)
  .setPreference(SpacesAroundMultiImports, false)

name := "spark-daria"

spName := "mrpowers/spark-daria"

spShortDescription := "Open source Spark transformations and functions"

spDescription := "When the Spark source code doesn't provide functionality, turn to this library"

version := "0.7.0"

scalaVersion := "2.11.8"
sparkVersion := "2.2.0"

sparkComponents ++= Seq("sql", "hive")

spDependencies += "MrPowers/spark-fast-tests:0.2.0"

libraryDependencies += "org.apache.commons" % "commons-text" % "1.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "provided"

parallelExecution in Test := false

// All Spark Packages need a license
licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")
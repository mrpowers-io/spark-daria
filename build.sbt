scalafmtOnCompile in Compile := true

organization := "com.github.mrpowers"
name := "spark-daria"

version := "0.38.0"
crossScalaVersions := Seq("2.11.12", "2.12.10")
scalaVersion := "2.11.12"
val sparkVersion = "2.4.4"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"

/*spName := "mrpowers/spark-daria"*/
/*spAppendScalaVersion := true*/

/*spShortDescription := "Spark helper methods to maximize developer productivity"*/

/*spDescription := "DataFrame validations, Column extensions, SparkSession extensions, sql functions, DataFrame transformations, and DataFrameHelpers."*/

libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "0.21.2"
libraryDependencies += "com.lihaoyi" %% "utest" % "0.6.3" % "test"
testFrameworks += new TestFramework("com.github.mrpowers.spark.daria.CustomFramework")

//artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
//  artifact.name + "-" + module.revision + "." + artifact.extension
//}

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  Artifact.artifactName(sv, module, artifact).replaceAll(s"-${module.revision}", s"-${sparkVersion}${module.revision}")
}

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

fork in Test := true

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled", "-Duser.timezone=GMT")

// All Spark Packages need a license
licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))

homepage := Some(url("https://github.com/MrPowers/spark-daria"))
developers ++= List(
  Developer("MrPowers", "Matthew Powers", "@MrPowers", url("https://github.com/MrPowers"))
)
scmInfo := Some(ScmInfo(url("https://github.com/MrPowers/spark-daria"), "git@github.com:MrPowers/spark-daria.git"))

updateOptions := updateOptions.value.withLatestSnapshots(false)

publishMavenStyle := true

publishTo := sonatypePublishToBundle.value

Global/useGpgPinentry := true

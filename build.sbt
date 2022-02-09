scalafmtOnCompile in Compile := true

organization := "com.github.mrpowers"
name := "spark-daria"

version := "1.2.3"
crossScalaVersions := Seq("2.12.15", "2.13.8")
scalaVersion := "2.12.15"
//scalaVersion := "2.13.8"
val sparkVersion = "3.2.1"

libraryDependencies += "org.apache.spark"    %% "spark-sql"        % sparkVersion % "provided"
libraryDependencies += "org.apache.spark"    %% "spark-mllib"      % sparkVersion % "provided"
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "1.1.0"      % "test"
libraryDependencies += "com.lihaoyi"         %% "utest"            % "0.7.11"      % "test"
libraryDependencies += "com.lihaoyi"         %% "os-lib"           % "0.8.0"      % "test"
testFrameworks += new TestFramework("com.github.mrpowers.spark.daria.CustomFramework")

credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")

fork in Test := true

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled", "-Duser.timezone=GMT")

licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))

homepage := Some(url("https://github.com/MrPowers/spark-daria"))
developers ++= List(
  Developer("MrPowers", "Matthew Powers", "@MrPowers", url("https://github.com/MrPowers"))
)
scmInfo := Some(ScmInfo(url("https://github.com/MrPowers/spark-daria"), "git@github.com:MrPowers/spark-daria.git"))

updateOptions := updateOptions.value.withLatestSnapshots(false)

publishMavenStyle := true

publishTo := sonatypePublishToBundle.value

Global / useGpgPinentry := true


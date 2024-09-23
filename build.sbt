scalafmtOnCompile in Compile := true

organization := "com.github.mrpowers"
name := "spark-daria"

version := "1.2.3"

val versionRegex = """^(.*)\.(.*)\.(.*)$""".r

val scala2_13 = "2.13.14"
val scala2_12 = "2.12.20"

val sparkVersion = System.getProperty("spark.testVersion", "3.3.4")
crossScalaVersions := {
  sparkVersion match {
    case versionRegex("3", m, _) if m.toInt >= 2 => Seq(scala2_12, scala2_13)
    case versionRegex("3", _, _) => Seq(scala2_12)
  }
}

scalaVersion := crossScalaVersions.value.head

libraryDependencies += "org.apache.spark"    %% "spark-sql"        % sparkVersion % "provided"
libraryDependencies += "org.apache.spark"    %% "spark-mllib"      % sparkVersion % "provided"
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "1.1.0"      % "test"
libraryDependencies += "com.lihaoyi"         %% "utest"            % "0.7.11"     % "test"
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


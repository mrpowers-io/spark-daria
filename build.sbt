scalafmtOnCompile in Compile := true

organization := "com.github.mrpowers"
name := "spark-daria"

version := "1.0.0"
crossScalaVersions := Seq("2.13.5", "2.12.12")
scalaVersion := crossScalaVersions.value.head
val sparkVersion = "3.0.1"

libraryDependencies += "org.apache.spark"    %% "spark-sql"        % sparkVersion % "provided"
libraryDependencies += "org.apache.spark"    %% "spark-mllib"      % sparkVersion % "provided"
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0"      % "test"
libraryDependencies += "com.lihaoyi"         %% "utest"            % "0.7.9"      % "test"
libraryDependencies += "com.lihaoyi"         %% "os-lib"           % "0.7.1"      % "test"
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

// sbt-ghpages plugin: https://github.com/sbt/sbt-ghpages
enablePlugins(SiteScaladocPlugin)
enablePlugins(GhpagesPlugin)
git.remoteRepo := "git@github.com:MrPowers/spark-daria.git"

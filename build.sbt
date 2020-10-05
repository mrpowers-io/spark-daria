scalafmtOnCompile in Compile := true

organization := "com.github.mrpowers"
name := "spark-daria"

version := "0.38.2"
crossScalaVersions := Seq("2.11.12", "2.12.10")
scalaVersion := "2.11.12"
val sparkVersion = "2.4.4"

libraryDependencies += "org.apache.spark"    %% "spark-sql"        % sparkVersion % "provided"
libraryDependencies += "org.apache.spark"    %% "spark-mllib"      % sparkVersion % "provided"
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "0.21.3"     % "test"
libraryDependencies += "com.lihaoyi"         %% "utest"            % "0.6.3"      % "test"
testFrameworks += new TestFramework("com.github.mrpowers.spark.daria.CustomFramework")

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  Artifact.artifactName(sv, module, artifact).replaceAll(s"-${module.revision}", s"-${sparkVersion}${module.revision}")
}

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

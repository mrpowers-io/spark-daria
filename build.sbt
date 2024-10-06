import scala.language.postfixOps

Compile / scalafmtOnCompile := true

organization := "com.github.mrpowers"
name := "spark-daria"

version := "1.2.3"

val versionRegex = """^(.*)\.(.*)\.(.*)$""".r

val scala2_13 = "2.13.15"
val scala2_12 = "2.12.20"

val sparkVersion = System.getProperty("spark.testVersion", "3.3.4")
crossScalaVersions := {
  sparkVersion match {
    case versionRegex("3", m, _) if m.toInt >= 2 => Seq(scala2_12, scala2_13)
    case versionRegex("3", _, _) => Seq(scala2_12)
    case versionRegex("4", _, _) => Seq(scala2_13)
  }
}

scalaVersion := crossScalaVersions.value.head

lazy val commonSettings = Seq(
  javaOptions ++= {
    Seq("-Xms512M", "-Xmx2048M", "-Duser.timezone=GMT") ++ (if (System.getProperty("java.version").startsWith("1.8.0"))
      Seq("-XX:+CMSClassUnloadingEnabled")
    else Seq.empty)
  },
  libraryDependencies ++= Seq(
    "org.apache.spark"    %% "spark-sql"        % sparkVersion % "provided",
    "org.apache.spark"    %% "spark-mllib"      % sparkVersion % "provided",
    "com.github.mrpowers" %% "spark-fast-tests" % "1.3.0"      % "test",
    "com.lihaoyi"         %% "utest"            % "0.8.2"      % "test",
    "com.lihaoyi"         %% "os-lib"           % "0.10.3"     % "test"
  ),
)

lazy val core = (project in file("core"))
  .settings(
    commonSettings,
    name := "core",
  )

lazy val unsafe = (project in file("unsafe"))
  .settings(
    commonSettings,
    name := "unsafe",
    Compile / unmanagedSourceDirectories ++= {
      sparkVersion match {
        case versionRegex(mayor, minor, _) =>
          (Compile / sourceDirectory).value ** s"*spark_*$mayor.$minor*" / "scala" get
      }
    },
    Test / unmanagedSourceDirectories ++= {
      sparkVersion match {
        case versionRegex(mayor, minor, _) =>
          (Compile / sourceDirectory).value ** s"*spark_*$mayor.$minor*" / "scala" get
      }
    },
  )

testFrameworks += new TestFramework("com.github.mrpowers.spark.daria.CustomFramework")

credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")

Test / fork := true

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


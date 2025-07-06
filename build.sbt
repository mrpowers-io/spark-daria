import scala.language.postfixOps

val versionRegex = """^(.*)\.(.*)\.(.*)$""".r
val scala2_13 = "2.13.15"
val scala2_12 = "2.12.20"
val sparkVersion = System.getProperty("spark.testVersion", "3.3.4")
val noPublish = Seq(
  (publish / skip) := true,
  publishArtifact  := false
)

inThisBuild(
  List(
    organization := "com.github.mrpowers",
    homepage     := Some(url("https://github.com/mrpowers-io/spark-daria")),
    licenses     := Seq("MIT" -> url("http://opensource.org/licenses/MIT")),
    developers ++= List(
      Developer("MrPowers", "Matthew Powers", "@MrPowers", url("https://github.com/MrPowers"))
    ),
    Compile / scalafmtOnCompile := true,
    Test / fork                 := true,
    crossScalaVersions := {
      sparkVersion match {
        case versionRegex("3", m, _) if m.toInt >= 2 => Seq(scala2_12, scala2_13)
        case versionRegex("3", _, _) => Seq(scala2_12)
        case versionRegex("4", _, _) => Seq(scala2_13)
      }
    },
    scalaVersion := crossScalaVersions.value.head
  )
)

lazy val commonSettings = Seq(
  javaOptions ++= {
    Seq("-Xms512M", "-Xmx2048M", "-Duser.timezone=GMT") ++ (if (System.getProperty("java.version").startsWith("1.8.0"))
      Seq("-XX:+CMSClassUnloadingEnabled")
    else Seq.empty)
  },
  libraryDependencies ++= Seq(
    "org.apache.spark"    %% "spark-sql"        % sparkVersion % "provided",
    "org.apache.spark"    %% "spark-mllib"      % sparkVersion % "provided",
    "com.github.mrpowers" %% "spark-fast-tests" % "3.0.1"      % "test",
    "com.lihaoyi"         %% "utest"            % "0.8.2"      % "test",
    "com.lihaoyi"         %% "os-lib"           % "0.10.3"     % "test"
  ),
)

lazy val core = (project in file("core"))
  .settings(
    commonSettings,
    moduleName                             := "spark-daria",
    name                                   := moduleName.value,
    Compile / packageSrc / publishArtifact := true,
    Compile / packageDoc / publishArtifact := true
  )

lazy val extension = (project in file("unsafe"))
  .settings(
    commonSettings,
    moduleName                             := "spark-daria-ext",
    name                                   := moduleName.value,
    Compile / packageSrc / publishArtifact := true,
    Compile / packageDoc / publishArtifact := true,
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
lazy val root = (project in file("."))
  .settings(
    name := "spark-daria-root",
    commonSettings,
    noPublish
  )
  .aggregate(core, extension)

testFrameworks += new TestFramework("com.github.mrpowers.spark.daria.CustomFramework")

scmInfo := Some(ScmInfo(url("https://github.com/MrPowers/spark-daria"), "git@github.com:MrPowers/spark-daria.git"))

updateOptions := updateOptions.value.withLatestSnapshots(false)

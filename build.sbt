import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

SbtScalariform.scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(DoubleIndentClassDeclaration, true)
  .setPreference(SpacesAroundMultiImports, false)

resolvers += "jitpack" at "https://jitpack.io"

name := "spark-daria"

version := "2.3.0_0.18.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0" % "provided"

libraryDependencies += "com.github.mrpowers" % "spark-fast-tests" % "v2.3.0_0.8.0" % "test"

libraryDependencies += "org.apache.commons" % "commons-text" % "1.1" % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

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

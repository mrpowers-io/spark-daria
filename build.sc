import mill._, scalalib._, publish._
import mill.scalalib.api.Util.{scalaBinaryVersion => binaryVersion}
import coursier.MavenRepository
import $ivy.`com.nikvanderhoof::mill-spark:0.1.0`
import com.nikvanderhoof.mill._

val dariaBuildMatrix = for {
  scala <- Seq("2.11.8", "2.12.8")
  spark <- Seq("2.1.0", "2.2.0", "2.3.1", "2.4.2")
  if !(scala >= "2.12.0" && spark < "2.4.0")
} yield (scala, spark)

object daria extends Cross[DariaModule](dariaBuildMatrix: _*)

class DariaModule(val crossScalaVersion: String, val crossSparkVersion: String)
extends CrossScalaSparkModule with PublishModule {
  def publishVersion = s"0.31.0_spark${binaryVersion(crossSparkVersion)}"
  def artifactName = "spark-daria"
  override def pomSettings = PomSettings(
    description = "Spark helper methods to maximize developer productivity.",
    organization = "com.github.mrpowers",
    url = "https://www.github.com/mrpowers/spark-daria",
    licenses = Seq(License.MIT),
    versionControl = VersionControl.github("mrpowers", "spark-daria"),
    developers = Seq(
      Developer("mrpowers", "Matthew Powers", "https://www.github.com/mrpowers")
    )
  )
  def compileIvyDeps = Agg(
    spark"sql",
    spark"mllib"
  )
  def repositories = super.repositories ++
    Seq(MavenRepository("https://dl.bintray.com/spark-packages/maven"))
  object test extends Tests {
    def ivyDeps = Agg(
      ivy"com.lihaoyi::utest:0.6.3",
      ivy"MrPowers:spark-fast-tests:0.19.1-s_${binaryVersion(crossScalaVersion)}",
      spark"sql",
      spark"mllib"
    )
    def testFrameworks = Seq("com.github.mrpowers.spark.daria.CustomFramework")
  }
}

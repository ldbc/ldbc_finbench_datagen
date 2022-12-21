package ldbc.finbench.datagen.util

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.sql.SparkSession

trait SparkApp {
  def appName: String

  type ArgsType

  /**
    * execute the data generation process
    */
  def run(args: ArgsType): Unit

  /**
    * set the {@link SparkConf}
    */
  val sparkConf = setConf(new SparkConf(), defaultSparkConf)

  /**
    * spark entry {@link SparkSession}
    */
  implicit def spark: SparkSession =
    SparkSession
      .builder()
      .master("local")
      .appName(appName)
      .config(sparkConf)
      .getOrCreate()

  private def applySparkConf(sparkConf: Map[String, String])(builder: SparkSession.Builder) =
    sparkConf.foldLeft(builder) { case (b, (k, v)) => b.config(k, v) }

  def setConf(sparkConf: SparkConf, conf: Map[String, String]): SparkConf = {
    conf.map(entry => {
      if (!sparkConf.contains(entry._1)) {
        sparkConf.set(entry._1, entry._2)
      }
    })
    sparkConf
  }

  def defaultSparkConf: Map[String, String] = Map(
    "spark.sql.session.timeZone" -> "GMT"
  )

  protected lazy val env: SparkEnv = new SparkEnv

}

trait DatagenStage extends SparkApp {
  override val appName: String =
    s"LDBC Finbench Datagen for Spark: ${this.getClass.getSimpleName.stripSuffix("$")}"
}

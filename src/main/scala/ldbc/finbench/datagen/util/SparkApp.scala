package ldbc.finbench.datagen.util

import org.apache.spark.SparkConf
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
      .appName(appName)
      .config(sparkConf)
      .getOrCreate()

  private def applySparkConf(sparkConf: Map[String, String])(builder: SparkSession.Builder) =
    sparkConf.foldLeft(builder) { case (b, (k, v)) => b.config(k, v) }

  def setConf(sparkConf: SparkConf, conf: Map[String, String]): SparkConf = {
    conf.map(entry => { sparkConf.set(entry._1, conf.getOrElse(entry._2, null)) })
    sparkConf
  }

  def defaultSparkConf: Map[String, String] = Map(
    "spark.sql.session.timeZone" -> "GMT"
  )

}

trait DatagenStage extends SparkApp {
  override val appName: String =
    s"LDBC Finbench Datagen for Spark: ${this.getClass.getSimpleName.stripSuffix("$")}"
}

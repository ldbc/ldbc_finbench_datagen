package ldbc.finbench.datagen.util

import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

class SparkEnv(implicit spark: SparkSession) {
  private val sysenv       = System.getenv().asScala
  private val invalidChars = raw"[.-]"

  def env(key: String): Option[String] = {
    sysenv
      .get(s"LDBC_FINBENCH_DATAGEN_${camelToUpper(key.replaceAll(invalidChars, "_"))}")
      .orElse(spark.conf.getOption(s"spark.ldbc.finbench.datagen.$key"))
  }

  val irFormat = env("irFormat").getOrElse("parquet")
}

package ldbc.finbench.datagen.util

import org.apache.spark.sql.SparkSession

object SparkUI {
  def job[T](jobGroup: String, jobDescription: String)(action: => T)(
      implicit spark: SparkSession): T = {
    spark.sparkContext.setJobGroup(jobGroup, jobDescription)
    try {
      action
    } finally {
      spark.sparkContext.clearJobGroup()
    }
  }
}

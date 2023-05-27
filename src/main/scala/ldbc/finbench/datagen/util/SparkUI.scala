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

  def jobAsync(jobGroup: String, jobDescription: String)(action: => Unit)(
    implicit spark: SparkSession): Unit = {
    spark.sparkContext.setJobGroup(jobGroup, jobDescription)
    try {
      new Thread(new Runnable {
        override def run(): Unit = {
          action
        }
      }).start()
    } finally {
      spark.sparkContext.clearJobGroup()
    }
  }
}

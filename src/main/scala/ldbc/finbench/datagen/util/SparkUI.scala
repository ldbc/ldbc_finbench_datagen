package ldbc.finbench.datagen.util

import org.apache.spark.sql.SparkSession
import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global

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
    implicit spark: SparkSession): Future[Unit] = {
    spark.sparkContext.setJobGroup(jobGroup, jobDescription)
    val future = Future {
      action
    }
    future.onComplete { _ => 
      spark.sparkContext.clearJobGroup()
    }
    future
  }
}

package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.config.DatagenConfiguration
import ldbc.finbench.datagen.entities.nodes.Medium
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters.asScalaIteratorConverter

object SparkMediumGenerator {
  def apply(mediumNums: Long, blockSize: Int, numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[Medium] = {
    val numBlocks = Math.ceil(mediumNums / blockSize.toDouble).toInt

    val mediumPartitionGenerator = (blocks: Iterator[Long]) => {
      val mediumGenerator = new MediumGenerator()

      for {
        i <- blocks
        size = Math.min(mediumNums - blockSize * i, blockSize)
        medium <- mediumGenerator.generateMediumBlock(i.toInt, blockSize).asScala.take(size.toInt)
      } yield medium
    }

    val partitions = numPartitions.getOrElse(spark.sparkContext.defaultParallelism)

    spark.sparkContext
      .range(0, numBlocks, step = 1, numSlices = partitions)
      .mapPartitions(mediumPartitionGenerator)
  }
}

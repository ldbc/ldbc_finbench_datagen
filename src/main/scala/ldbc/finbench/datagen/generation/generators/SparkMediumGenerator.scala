package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.config.DatagenConfiguration
import ldbc.finbench.datagen.entities.nodes.Medium
import ldbc.finbench.datagen.generation.DatagenContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters.asScalaIteratorConverter

object SparkMediumGenerator {
  def apply(
      mediumNums: Long,
      config: DatagenConfiguration,
      blockSize: Int,
      numPartitions: Option[Int] = None
  )(implicit spark: SparkSession): RDD[Medium] = {

    val numBlocks = Math.ceil(mediumNums / blockSize.toDouble).toInt

    val mediumPartitionGenerator = (blocks: Iterator[Long]) => {
      // OPT: It is called in each SparkGenerator in Spark to initialize the context on the executors.
      // 1. Make the context as an object instead of a static class
      // 2. Pass the context to SparkContext instead of
      DatagenContext.initialize(config)
      val mediumGenerator = new MediumGenerator()

      for {
        i <- blocks
        size = Math.min(mediumNums - blockSize * i, blockSize)
        medium <- mediumGenerator
          .generateMediumBlock(i.toInt, blockSize)
          .asScala
          .take(size.toInt)
      } yield medium
    }

    val partitions =
      numPartitions.getOrElse(spark.sparkContext.defaultParallelism)

    spark.sparkContext
      .range(0, numBlocks, step = 1, numSlices = partitions)
      .mapPartitions(mediumPartitionGenerator)
  }
}

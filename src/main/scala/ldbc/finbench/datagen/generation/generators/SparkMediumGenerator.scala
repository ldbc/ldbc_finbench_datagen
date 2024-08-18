package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.config.DatagenConfiguration
import ldbc.finbench.datagen.entities.nodes.Medium
import ldbc.finbench.datagen.generation.DatagenContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters.asScalaIteratorConverter

object SparkMediumGenerator {
  def apply(
      numMedia: Long,
      config: DatagenConfiguration,
      blockSize: Int
  )(implicit spark: SparkSession): RDD[Medium] = {
    val numBlocks = Math.ceil(numMedia / blockSize.toDouble).toInt
    val partitions = Math.min(numBlocks, spark.sparkContext.defaultParallelism)

    spark.sparkContext
      .range(0, numBlocks, step = 1, numSlices = partitions)
      .mapPartitions { blocks =>
        DatagenContext.initialize(config)
        val mediumGenerator = new MediumGenerator()

        blocks.flatMap { i =>
          val size = Math.min(numMedia - blockSize * i, blockSize)
          mediumGenerator
            .generateMediumBlock(i.toInt, blockSize)
            .asScala
            .take(size.toInt)
        }
      }
  }
}

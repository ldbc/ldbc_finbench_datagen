package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.config.DatagenConfiguration
import ldbc.finbench.datagen.entities.nodes.Person
import ldbc.finbench.datagen.generation.DatagenContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters.asScalaIteratorConverter

object SparkPersonGenerator {
  def apply(numPersons: Long, config: DatagenConfiguration, blockSize: Int)(
      implicit spark: SparkSession
  ): RDD[Person] = {
    val numBlocks = Math.ceil(numPersons / blockSize.toDouble).toInt
    val partitions = Math.min(numBlocks, spark.sparkContext.defaultParallelism)

    spark.sparkContext
      .range(0, numBlocks, step = 1, numSlices = partitions)
      .mapPartitions { blocks =>
        DatagenContext.initialize(config)
        val personGenerator = new PersonGenerator()

        blocks.flatMap { i =>
          val size = Math.min(numPersons - blockSize * i, blockSize)
          personGenerator
            .generatePersonBlock(i.toInt, blockSize)
            .asScala
            .take(size.toInt)
        }
      }
  }
}

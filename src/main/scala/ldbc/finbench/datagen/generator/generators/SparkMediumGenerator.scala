package ldbc.finbench.datagen.generator.generators

import ldbc.finbench.datagen.entities.nodes.{Company, Medium}
import ldbc.finbench.datagen.util.GeneratorConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters.asScalaIteratorConverter

object SparkMediumGenerator {
  def apply(conf: GeneratorConfiguration, numPartitions: Option[Int] = None)(
      implicit spark: SparkSession): RDD[Medium] = {
    val mediumNums = 10000 // todo use parameter
    val blockSize  = 5000 // todo use parameter
    val numBlocks  = Math.ceil(mediumNums / blockSize).toInt

    val mediumPartitionGenerator = (blocks: Iterator[Long]) => {
      val mediumGenerator = new MediumGenerator(conf)

      for {
        i <- blocks
        size = Math.min(mediumNums - blockSize * i, blockSize)
        company <- mediumGenerator
          .generateMediumBlock(i.toInt, blockSize)
          .asScala
          .take(size.toInt)
      } yield company
    }

    val partitions = numPartitions.getOrElse(spark.sparkContext.defaultParallelism)

    val mediumRdd = spark.sparkContext
      .range(0, numBlocks, step = 1, numSlices = partitions)
      .mapPartitions(mediumPartitionGenerator)
    mediumRdd
  }
}

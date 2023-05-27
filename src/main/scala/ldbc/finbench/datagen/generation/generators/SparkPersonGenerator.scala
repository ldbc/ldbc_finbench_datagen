package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.config.DatagenConfiguration
import ldbc.finbench.datagen.entities.nodes.Person
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters.asScalaIteratorConverter

object SparkPersonGenerator {
  def apply(numPersons: Long, blockSize: Int, numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[Person] = {
    val numBlocks = Math.ceil(numPersons / blockSize.toDouble).toInt

    val personPartitionGenerator = (blocks: Iterator[Long]) => {
      val personGenerator = new PersonGenerator()

      for {
        i <- blocks
        size = Math.min(numPersons - blockSize * i, blockSize)
        person <- personGenerator.generatePersonBlock(i.toInt, blockSize).asScala.take(size.toInt)
      } yield person
    }

    val partitions = numPartitions.getOrElse(spark.sparkContext.defaultParallelism)

    spark.sparkContext
      .range(0, numBlocks, step = 1, numSlices = partitions)
      .mapPartitions(personPartitionGenerator)
  }
}

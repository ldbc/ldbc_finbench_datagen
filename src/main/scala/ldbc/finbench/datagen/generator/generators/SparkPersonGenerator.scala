package ldbc.finbench.datagen.generator.generators

import ldbc.finbench.datagen.entities.nodes.Person
import ldbc.finbench.datagen.generator.DatagenContext
import ldbc.finbench.datagen.util.GeneratorConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters.asScalaIteratorConverter

object SparkPersonGenerator {
  def apply(conf: GeneratorConfiguration, numPartitions: Option[Int] = None)(
      implicit spark: SparkSession): RDD[Person] = {

    val personNums = 10 //TODO DatagenParams.numPersons
    val blockSize  = 5; //TODO DatagenParams.blockSize
    val numBlocks  = Math.ceil(personNums / blockSize).toInt

    val personPartitionGenerator = (blocks: Iterator[Long]) => {
      DatagenContext.initialize(conf)
      val personGenerator =
        new PersonGenerator(conf, conf.get("generator.distribution.degreeDistribution"))

      for {
        i <- blocks
        size = Math.min(personNums - blockSize * i, blockSize)
        person <- personGenerator.generatePersonBlock(i.toInt, blockSize).asScala.take(size.toInt)
      } yield person
    }

    val partitions = numPartitions.getOrElse(spark.sparkContext.defaultParallelism)

    spark.sparkContext
      .range(0, numBlocks, step = 1, numSlices = partitions)
      .mapPartitions(personPartitionGenerator)
  }
}

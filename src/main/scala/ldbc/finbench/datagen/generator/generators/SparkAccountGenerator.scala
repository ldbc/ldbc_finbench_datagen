package ldbc.finbench.datagen.generator.generators

import ldbc.finbench.datagen.entities.nodes.Account
import ldbc.finbench.datagen.generator.{DatagenContext, DatagenParams}
import ldbc.finbench.datagen.util.GeneratorConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters.asScalaIteratorConverter

object SparkAccountGenerator {
  def apply(conf: GeneratorConfiguration, numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[Account] = {

    val blockSize = DatagenParams.blockSize;
    val numAccountBlocks = Math.ceil(DatagenParams.numAccounts / blockSize).toInt
    val accountPartitionGenerator = (blocks: Iterator[Long]) => {
      DatagenContext.initialize(conf)
      val accountGenerator = new AccountGenerator(conf)
      for {
        i <- blocks
        size = Math.min(numAccountBlocks - blockSize * i, blockSize)
        account <- accountGenerator.generateAccountBlock(i.toInt, blockSize).asScala.take(size.toInt)
      } yield account
    }
    val partitions = numPartitions.getOrElse(spark.sparkContext.defaultParallelism)
    val accountRdd = spark.sparkContext
      .range(0, numAccountBlocks, step = 1, numSlices = partitions)
      .mapPartitions(accountPartitionGenerator)

    accountRdd
  }
}

package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.config.DatagenConfiguration
import ldbc.finbench.datagen.entities.nodes.Account
import ldbc.finbench.datagen.generation.{DatagenContext, DatagenParams}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters.asScalaIteratorConverter

// SparkAccountGenerator is not used to generate account data directly.
object SparkAccountGenerator {
//  def apply(conf: DatagenConfiguration, numPartitions: Option[Int] = None)(
//    implicit spark: SparkSession): RDD[Account] = {
//    val numAccounts = 10000
//
//    val accountPartitionGenerator = (blocks: Iterator[Long]) => {
//      DatagenContext.initialize(conf)
//      val accountGenerator = new AccountGenerator()
//      for {
//        i <- blocks
//        size = Math.min(numAccounts - DatagenParams.blockSize * i, DatagenParams.blockSize)
//        account <- accountGenerator.
//      } yield account
//    }
//    val numAccountBlocks = Math.ceil(numAccounts / DatagenParams.blockSize.toDouble).toInt
//    val partitions = numPartitions.getOrElse(spark.sparkContext.defaultParallelism)
//    val accountRdd = spark.sparkContext
//      .range(0, numAccountBlocks, step = 1, numSlices = partitions)
//      .mapPartitions(accountPartitionGenerator)
//
//    accountRdd
//  }
}

package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.config.DatagenConfiguration
import ldbc.finbench.datagen.entities.nodes.Company
import ldbc.finbench.datagen.generation.DatagenContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters.asScalaIteratorConverter

object SparkCompanyGenerator {
  def apply(
      companyNums: Long,
      config: DatagenConfiguration,
      blockSize: Int,
      numPartitions: Option[Int] = None
  )(implicit spark: SparkSession): RDD[Company] = {

    val numBlocks = Math.ceil(companyNums / blockSize.toDouble).toInt

    val companyPartitionGenerator = (blocks: Iterator[Long]) => {
      // OPT: It is called in each SparkGenerator in Spark to initialize the context on the executors.
      // 1. Make the context as an object instead of a static class
      // 2. Pass the context to SparkContext instead of
      DatagenContext.initialize(config)

      val companyGenerator = new CompanyGenerator()

      for {
        i <- blocks
        size = Math.min(companyNums - blockSize * i, blockSize)
        company <- companyGenerator
          .generateCompanyBlock(i.toInt, blockSize)
          .asScala
          .take(size.toInt)
      } yield company
    }

    val partitions =
      numPartitions.getOrElse(spark.sparkContext.defaultParallelism)

    spark.sparkContext
      .range(0, numBlocks, step = 1, numSlices = partitions)
      .mapPartitions(companyPartitionGenerator)
  }
}

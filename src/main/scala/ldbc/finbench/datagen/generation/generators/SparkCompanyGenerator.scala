package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.config.DatagenConfiguration
import ldbc.finbench.datagen.entities.nodes.Company
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters.asScalaIteratorConverter

object SparkCompanyGenerator {
  def apply(companyNums: Long, blockSize: Int, numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[Company] = {
    val numBlocks = Math.ceil(companyNums / blockSize.toDouble).toInt

    val companyPartitionGenerator = (blocks: Iterator[Long]) => {
      val companyGenerator = new CompanyGenerator()

      for {
        i <- blocks
        size = Math.min(companyNums - blockSize * i, blockSize)
        company <- companyGenerator.generateCompanyBlock(i.toInt, blockSize).asScala.take(size.toInt)
      } yield company
    }

    val partitions = numPartitions.getOrElse(spark.sparkContext.defaultParallelism)

    spark.sparkContext
      .range(0, numBlocks, step = 1, numSlices = partitions)
      .mapPartitions(companyPartitionGenerator)
  }
}

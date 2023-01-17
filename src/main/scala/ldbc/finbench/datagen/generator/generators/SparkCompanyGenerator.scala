package ldbc.finbench.datagen.generator.generators

import ldbc.finbench.datagen.entities.nodes.Company
import ldbc.finbench.datagen.util.GeneratorConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters.asScalaIteratorConverter

object SparkCompanyGenerator {
  def apply(conf: GeneratorConfiguration, numPartitions: Option[Int] = None)(
      implicit spark: SparkSession): RDD[Company] = {
    val companyNums = 10000
    val blockSize   = 5000
    val numBlocks   = Math.ceil(companyNums / blockSize).toInt

    val companyPartitionGenerator = (blocks: Iterator[Long]) => {
      val companyGenerator = new CompanyGenerator(conf)

      for {
        i <- blocks
        size = Math.min(companyNums - blockSize * i, blockSize)
        company <- companyGenerator
          .generateCompanyBlock(i.toInt, blockSize)
          .asScala
          .take(size.toInt)
      } yield company
    }

    val partitions = numPartitions.getOrElse(spark.sparkContext.defaultParallelism)

    val companyRdd = spark.sparkContext
      .range(0, numBlocks, step = 1, numSlices = partitions)
      .mapPartitions(companyPartitionGenerator)
    companyRdd
  }
}

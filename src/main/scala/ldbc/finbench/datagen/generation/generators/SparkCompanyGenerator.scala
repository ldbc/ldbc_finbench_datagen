package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.config.DatagenConfiguration
import ldbc.finbench.datagen.entities.nodes.Company
import ldbc.finbench.datagen.generation.DatagenContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters.asScalaIteratorConverter

object SparkCompanyGenerator {
  def apply(
      numCompanies: Long,
      config: DatagenConfiguration,
      blockSize: Int
  )(implicit spark: SparkSession): RDD[Company] = {
    val numBlocks = Math.ceil(numCompanies / blockSize.toDouble).toInt
    val partitions = Math.min(numBlocks, spark.sparkContext.defaultParallelism)

    spark.sparkContext
      .range(0, numBlocks, step = 1, numSlices = partitions)
      .mapPartitions { blocks =>
        DatagenContext.initialize(config)
        val companyGenerator = new CompanyGenerator()

        blocks.flatMap { i =>
          val size = Math.min(numCompanies - blockSize * i, blockSize)
          companyGenerator
            .generateCompanyBlock(i.toInt, blockSize)
            .asScala
            .take(size.toInt)
        }
      }
  }
}

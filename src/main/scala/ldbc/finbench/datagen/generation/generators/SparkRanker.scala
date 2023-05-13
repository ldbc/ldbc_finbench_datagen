package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.nodes.Person
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.SortedMap
import scala.reflect.ClassTag

trait SparkRanker {
  def apply(persons: RDD[Person]): RDD[(Long, Person)]
}

object SparkRanker {

  def create[K: Ordering: ClassTag](by: Person => K)(implicit spark: SparkSession): SparkRanker =
    new SparkRanker {
      override def apply(persons: RDD[Person]): RDD[(Long, Person)] = {
        val sortedPersons = persons.sortBy(by).cache()

        // single count / partition. Assumed small enough to collect and broadcast
        val counts = sortedPersons
          .mapPartitionsWithIndex((i, ps) => Array((i, ps.size)).iterator,
                                  preservesPartitioning = true)
          .collectAsMap()

        val aggregatedCounts = SortedMap(counts.toSeq: _*)
          .foldLeft((0L, Map.empty[Int, Long])) {
            case ((total, map), (i, c)) =>
              (total + c, map + (i -> total))
          }
          ._2

        val broadcastedCounts = spark.sparkContext.broadcast(aggregatedCounts)

        sortedPersons.mapPartitionsWithIndex((i, ps) => {
          val start = broadcastedCounts.value(i)
          for { (p, j) <- ps.zipWithIndex } yield (start + j, p)
        })
      }
    }
}

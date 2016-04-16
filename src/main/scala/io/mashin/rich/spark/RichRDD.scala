package io.mashin.rich.spark

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object RichRDD {
  implicit def rddToRichRDDFunctions[T: ClassTag](rdd: RDD[T]): RichRDDFunctions[T] =
    new RichRDDFunctions[T](rdd)

  implicit def pairRDDToRichPairRDDFunctions[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)])
    : RichPairRDDFunctions[K, V] = new RichPairRDDFunctions[K, V](rdd)
}

package io.mashin.rich.spark;

import static org.junit.Assert.assertArrayEquals;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;

public class TestJavaRichRDD {

  @Test
  public void testScanLeftJavaRDD() {
    JavaSparkContext sc = sc("testScanLeftJavaRDD");

    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1), 4);
    rdd = JavaRichRDD.scanLeft(rdd, 0, 1, (a, b) -> a + b);

    assertArrayEquals(new Integer[] {1, 2, 3, 4, 5, 6, 7, 8, 9},
        rdd.collect().toArray(new Integer[0]));

    sc.stop();
  }

  @Test
  public void testScanRightJavaRDD() {
    JavaSparkContext sc = sc("testScanRightJavaRDD");

    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1), 4);
    rdd = JavaRichRDD.scanRight(rdd, 0, 1, (a, b) -> a + b);

    assertArrayEquals(new Integer[] {9, 8, 7, 6, 5, 4, 3, 2, 1},
        rdd.collect().toArray(new Integer[0]));

    sc.stop();
  }

  @Test
  public void testScanLeftJavaPairRDD() {
    JavaSparkContext sc = sc("testScanLeftJavaPairRDD");

    JavaPairRDD<Integer, Integer> rdd = sc.parallelizePairs(Arrays.asList(
        t(1, 1), t(2, 1), t(3, 1), t(4, 1), t(5, 1), t(6, 1), t(7, 1), t(8, 1)), 4);
    rdd = JavaRichRDD.scanLeft(rdd, 0, 0, 1, (a, b) -> a + b);

    assertArrayEquals(new Object[] {
        t(0, 1), t(1, 2), t(2, 3), t(3, 4), t(4, 5), t(5, 6), t(6, 7), t(7, 8), t(8, 9)
      }, rdd.collect().toArray(new Object[0]));

    sc.stop();
  }

  @Test
  public void testScanRightJavaPairRDD() {
    JavaSparkContext sc = sc("testScanRightJavaPairRDD");

    JavaPairRDD<Integer, Integer> rdd = sc.parallelizePairs(Arrays.asList(
        t(1, 1), t(2, 1), t(3, 1), t(4, 1), t(5, 1), t(6, 1), t(7, 1), t(8, 1)), 4);
    rdd = JavaRichRDD.scanRight(rdd, 0, 0, 1, (a, b) -> a + b);

    assertArrayEquals(new Object[] {
        t(1, 9), t(2, 8), t(3, 7), t(4, 6), t(5, 5), t(6, 4), t(7, 3), t(8, 2), t(0, 1)
    }, rdd.collect().toArray(new Object[0]));

    sc.stop();
  }

  private <K, V> Tuple2<K, V> t(K k, V v) {
    return new Tuple2<>(k, v);
  }

  private JavaSparkContext sc(String name) {
    return new JavaSparkContext(new SparkConf().setMaster("local[*]").setAppName(name));
  }

}

package sssp

import org.apache.spark.sql.SparkSession

object SparkSSSP {

    def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("SingleSourceShortestPath")
      .getOrCreate()

    val lines = spark.read.textFile(args(0)).rdd

    val links = lines.map{ s =>
      val parts = s.split(",")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()


    links.saveAsTextFile(args(1))

  }
}

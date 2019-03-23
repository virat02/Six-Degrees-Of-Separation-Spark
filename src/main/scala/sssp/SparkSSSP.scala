package sssp

import org.apache.spark.sql.SparkSession

object SparkSSSP {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("SingleSourceShortestPath")
      .getOrCreate()

    val lines = spark.read.textFile(args(0)).rdd

    val graph = lines.map { s =>
      val parts = s.split(",")
      (parts(0), parts(1))
    }
      .distinct()
      .groupByKey()
      .cache()

    val distances = graph
      .map( x => if (x._1.toInt == 1) (x._1,0) else (x._1,-1))




    distances.saveAsTextFile(args(1))

        }
  }

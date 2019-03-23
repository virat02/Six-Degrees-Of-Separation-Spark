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

    var distances = graph
      .map( x => if (x._1.toInt == 1) (x._1,0) else (x._1,-1))

    var temp = distances

    for (iterationCount<-1 to 10) {

      temp = graph.join(temp)
        .flatMap(x => x._2._1
          .map(y => if (x._2._2 == -1){
            (y, -1)
          } else {
            (y, x._2._2 + 1)
          })
        )

      distances = temp.union(distances)

      }

    println(distances.collect().foreach(println))
    distances.saveAsTextFile(args(1))
  }
}

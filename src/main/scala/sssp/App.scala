package sssp

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

/**
  * @author ${user.name}
  */
object App {

  def getMin(a: Int, b: Int): Int ={
    if (a == -1){
      return b
    }

    if (b == -1){
      return a
    }

    if (a < b) a else b
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SingleSourceShortestPath")
    conf.set("spark.eventLog.enabled","true")
    conf.set("spark.eventLog.dir","eventlog")
    val spark = new SparkContext(conf)

    val lines = spark.textFile(args(0))
    val MAX = 50

    val graph = lines.map { s =>
      val parts = s.split(",")
      (parts(0), parts(1))
    }
      .filter(x => x._1.toInt <= MAX && x._2.toInt <= MAX)
      .distinct()
      .groupByKey()
      .cache()

    var distances = graph
      .map( x => if (x._1.toInt == 1) (x._1,0) else (x._1,-1))

    var temp = distances

    var done = false
    while(!done) {

      temp = graph.join(temp)
        .flatMap(x => x._2._1
          .map(y => if (x._2._2 == -1){
            (y, -1)
          } else {
            (y, x._2._2 + 1)
          })
        )

      val distances1 = temp.union(distances).reduceByKey((x,y) => getMin(x,y))

      done = distances.join(distances1)
        .map{case (x,y) => y._1 == y._2}
        .reduce((x,y) => x && y)

      distances = distances1
    }
    println(distances.collect().foreach(println))
    distances.saveAsTextFile(args(1))
  }
}

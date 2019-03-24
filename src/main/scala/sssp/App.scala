package sssp

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import util.control.Breaks._

/**
  * @author ${user.name}
  */
object App {

  //"-1" denotes positive infinity
  def getMin(a: Int, b: Int): Int ={
    if (a == -1) return b
    if (b == -1) return a

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
      //Filter out values above the threshold MAX
      .filter(x => x._1.toInt <= MAX && x._2.toInt <= MAX)

      //pre-process the data to remove duplicates
      .distinct()

      //get the adjacency list
      .groupByKey()

      //cache the RDD
      .cache()

    //Get the initial distance RDD
    var distances = graph
      .map( x => if (x._1.toInt == 1) (x._1,0) else (x._1,-1))

    //distances will update after each iteration
    var temp = distances

    breakable {
      while(true) {

        //distances, through different existing paths, for all the nodes reached so far
        temp = graph.join(temp)
          .filter(x => x._2._2 != -1)
          .flatMap(x => x._2._1
            .map(y => (y, x._2._2 + 1)))

        //updated distances for all nodes reached so far
        val distances1 = temp.union(distances).reduceByKey((x, y) => getMin(x, y))

        //check if any distance for any node has changed,
        // as well as all nodes are visited from the given source to all it's targets
        val done = distances.join(distances1)
          .map { case (x, y) => y._1 == y._2 }
          .reduce((x, y) => x && y)

        //Update the distances RDD only if it has changed from it's previous state
        if (!done) distances = distances1 else break
      }
    }

    //save the output
    distances.saveAsTextFile(args(1))
  }
}

package pr

import java.text.DecimalFormat

import org.apache.commons.math3.util.Precision
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD

object Pagerank {

  //returns square of the integer input
  def pow2(v: Int)= v*v

  //Make the row of our graph based on the input parameter k
  def makeRow(x: Int, k: Int, sc: SparkContext):  RDD[(Int,Int)] = {
    var i = x

    var row: List[(Int, Int)] = List()
    while(i <= k) {

      if(i == k){
        row = row:+(i,0)
      }
      else {
        row = row:+(i, i+1)
      }
      i+=1
    }

    val s = sc.parallelize(row)

    s
  }

  //Make the input based on the input parameter k and store it in input file input.csv
  def makeGraph(k: Int, input: String, sc: SparkContext):  RDD[(Int,Int)] = {

    var listOfRecords = sc.emptyRDD[(Int, Int)]

    for(i <- 1 to pow2(k) by k) {
      listOfRecords = listOfRecords union makeRow(i, i+k-1, sc)
    }

    listOfRecords
  }
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\npr.Pagerank <input dir> <output dir> <param-k>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Pagerank")
    val sc = new SparkContext(conf)

    val param = args{2}.toInt
    val graphRDD = makeGraph(param, args{0}, sc)

    //persist graphRDD
    graphRDD.persist()

    val zeroRow: List[(Int, Float)] = List()
    val dummyNodeRDD = sc.parallelize(zeroRow:+(0,0f))

    val rankRDD = graphRDD.map(x =>  (x._1,1f/pow2(param)))

    //var since this RDD will have updated PR values after every iteration
    var finalRankRDD = rankRDD union dummyNodeRDD

    var noInlinkNodes: List[(Int, Float)] = List()

    //assign a default pagerank of 0 to all the nodes with no in-links as they contains no contributions
    for (i <- 1 to pow2(param) by param) {
      noInlinkNodes = noInlinkNodes :+ (i, 0f)
    }

    //form the noInlink RDD
    val noInlinkRDD = sc.parallelize(noInlinkNodes)

    //perform 10 iterations
    for(i <- 1 to 10) {

      val contributions = graphRDD.join(finalRankRDD)
        //returns each vertex id m in n’s adjacency list as (m, n’s PageRank / number of n’s out-links).
        .map(x => (x._2._1, x._2._2))

      var ranks = contributions.reduceByKey((x, y) => x + y)

      //add the pages with no inlinks along with their dummy pageranks of 0
      ranks = ranks union noInlinkRDD

      //fetch the delta
      val prOfDummy : Float = ranks.lookup(0).head

      //compute PR values without the delta contribution
      val withoutDanglingRDD = ranks.map(x => if(x._1 != 0) (x._1, (0.15/pow2(param).toFloat + 0.85*x._2).toFloat) else (x._1, prOfDummy.toFloat))

      //compute the delta value
      val delta = prOfDummy/pow2(param).toFloat*0.85

      //compute the final pagerank value for the current iteration
      finalRankRDD = withoutDanglingRDD.map(x => if (x._1 != 0) (x._1, x._2 + delta.toFloat) else (x._1, delta.toFloat))

      //round the PR value upto 15 decimal places
      finalRankRDD = finalRankRDD.mapValues(v => Precision.round(v , 15))

    }

    // Write out the final ranks
    finalRankRDD.saveAsTextFile(args{1})
  }
}
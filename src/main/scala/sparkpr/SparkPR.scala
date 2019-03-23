// Starter code of page rank from spark distribution used

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sparkpr

import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer

/**
  * Computes the PageRank of URLs from an input file. Input file should
  * be in format of:
  * URL         neighbor URL
  * URL         neighbor URL
  * URL         neighbor URL
  * ...
  * where URL and their neighbors are separated by space(s).
  *
  * This is an example implementation for learning how to use Spark. For more conventional use,
  * please refer to org.apache.spark.graphx.lib.PageRank
  *
  * Example Usage:
  * {{{
  * bin/run-example SparkPageRank data/mllib/pagerank_data.txt 10
  * }}}
  */
object SparkPR {

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of PageRank and is given as an example!
        |Please use the PageRank implementation found in org.apache.spark.graphx.lib.PageRank
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <iter>")
      System.exit(1)
    }

    showWarning()

    // value to create synthetic data
    var k = 100

    /*
    Reference - https://www.tutorialspoint.com/scala/
    For data structures etc in scala
     */
    // creating synthetic data
    val raw_data: ListBuffer[List[Int]] = ListBuffer()
    var len = k*k
    var count = 1
    var temp = k // counter for each chain

    while (count <= len){
      // making dangling vertex point to dummy vertex
      if (temp == 1){
        val temp_list: List[Int] = List(count, 0)
        raw_data += temp_list
        temp = k
        count = count + 1
      }
      // creating one chain
      else{
        val temp_list: List[Int] = List(count, count+1)
        raw_data += temp_list
        temp = temp - 1
        count = count + 1
      }
    }

    val conf = new SparkConf().setAppName("SparkPageRank")
    val sc = new SparkContext(conf)

    // converting synthetic data to rdd
    val lines = sc.parallelize(raw_data)

    val iters = if (args.length > 1) args(1).toInt else 10
    val links = lines.map(s => (s(0), s(1)))
      .distinct()
      .groupByKey()
      .cache()

    var ranks = links.map(v => (v._1, 1.0))
    var dummy_rank = sc.parallelize(List((0,0.0)))

    ranks = ranks.union(dummy_rank)

    // Number of iterations the page rank runs, can be changed in make file (default 10)
    for (i <- 1 to iters) {
      // joining our graph and our ranks rdd
      val contrib_join = links.leftOuterJoin(ranks)

        // calculating new rank o f vertices that were emitted in the join
        val contrib_1 = contrib_join
          .flatMap{ case (v, (urls, rank)) =>
            val size = urls.size
            urls
              .map(url => {
                (url, rank.getOrElse(0.0) / size)
              })
          }

        // Another rdd to get back vertices that were lost in the join
        val contrib_2 = contrib_join
          .map{case (v, (urls, rank)) => (v, 0.0)}

      val contribs = contrib_1.union(contrib_2)

      ranks = contribs.reduceByKey(_ + _)

      // distributing accumulated page rank in zero to all others
      // and also adding the random surfer probablity  to those vertices
      var rank_0 = ranks.lookup(0).apply(0)
      val share = rank_0 / (k*k)
      ranks = ranks.map{ x =>
        if (x._1 != 0){
          (x._1, 0.15 + 0.85*(x._2 + share))
        } else {
          (x._1,0)
        }
      }
    }

    // to answer question of getting page rank of 100 vertexes when k = 100
    //ranks = ranks.filter(x => x._1 <=100)

    logger.info("Important Log Here "+ranks.toDebugString)
    ranks.saveAsTextFile(args(2))

    // to print the ranks
    //val output = ranks.collect()
    //output.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))
  }
}
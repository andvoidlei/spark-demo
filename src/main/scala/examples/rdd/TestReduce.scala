package examples.rdd

import org.apache.spark.sql.SparkSession

object TestReduce {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("local")
      .appName("ALSExample")
      .getOrCreate()


    val words = Array("one", "two", "two", "three", "three", "three")

    val wordPairsRDD = spark.sparkContext.parallelize(words).map(word => (word, 1))

    val wordCountsWithReduce = wordPairsRDD.reduceByKey(_ + _)

    val wordCountsWithGroup = wordPairsRDD.groupByKey().map(t => (t._1, t._2.max)) //.map(t => (t._1, t._2.sum))
    //wordCountsWithReduce.foreach(println)
    //wordCountsWithGroup.foreach(println)
    wordPairsRDD.foreach(println)
    val arr = wordPairsRDD.collect()
    println("arr(0) is " + arr(0) + " arr(2) is " + arr(2) + " arr(4) is " + arr(4))
  }

  //def sumTot()

}

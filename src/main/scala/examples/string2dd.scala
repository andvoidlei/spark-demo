package examples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object string2dd {


  //val spark = SparkSession.builder().master("local").getOrCreate()

  val conf = new SparkConf().setMaster("local").setAppName("WordCount")

  //创建SparkCore的程序入口
  val sparkCon = new SparkContext(conf)


  def sortedTopOne(data: String): String = {
    println(data)
    var wordCount = data.split(",").map(arr => (arr, 1))
    println(wordCount)
    //var sc = spark.sparkContext.parallelize(wordCount)
    var sc = sparkCon.parallelize(wordCount)
    var countKey = sc.reduceByKey(_ + _)
    countKey.foreach(println)
    var result = countKey.sortBy(_._2, false).keys.collect().toList
    result.foreach(println)
    println("---" + result)
    var result1 = result(0)
    if (result1 == "-1") {
      result1 = result(1)
    }
    return result1
  }

  def main(args: Array[String]): Unit = {
    //val sc = spark.sparkContext
    var list = "332684,60070,306590,57608,-1,60070,60070,-1,302720,60070,-1,120537,63672,60848,99634,60070,60070,60795,56737,60070,41737,57085,304525,120516,-1,-1,-1,60070,-1,-1,-1,-1,-1,-1,-1,-1,-1,63673,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1"
    var result = sortedTopOne(list)
    println("xxx" + result)

  }

}

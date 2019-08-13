package examples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object testFlatMap {


  def main(args:Array[String]):Unit={

    var z = Array("hello world e", "hello fly", "a,b")
    //var z = "hello,e world,1 hello".split(" ")//.map(arr => (arr))


    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)


    var rdd1 = sc.parallelize(z)
    //rdd1.foreach(println)

    rdd1.map(_.split(" ")).foreach(println)
    println(">>>>>>>")
    rdd1.flatMap(_.split(" ")).foreach(println)


    val list = List("张无忌", "赵敏", "周芷若")
    val listRDD = sc.parallelize(list)
    val nameRDD = listRDD.map(name => "Hello " + name)
    nameRDD.foreach(name => println(name))


    val list1 = List("张无忌 赵敏","宋青书 周芷若")
    val listRDD1 = sc.parallelize(list1)

    val nameRDD1 = listRDD1.flatMap(line => line.split(" ")).map(name => "Hello " + name)
    nameRDD1.foreach(name => println(name))


  }
}

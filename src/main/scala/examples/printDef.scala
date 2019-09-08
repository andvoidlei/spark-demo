package examples

import org.apache.spark.sql.SparkSession

object printDef {


  def p(rdd: org.apache.spark.rdd.RDD[_]) = rdd.foreach(println)

  implicit class Printer(rdd: org.apache.spark.rdd.RDD[_]) {
    def print = rdd.foreach(println)
  }


  /** Usage: HdfsTest [file] */
  def main(args: Array[String]) {
    //    if (args.length < 1) {
    //      System.err.println("Usage: HdfsTest <file>")
    //      System.exit(1)
    //    }
    val spark = SparkSession
      .builder
      .master("local")
      .appName("HdfsTest")
      .getOrCreate()
    //val dir="hdfs://nameservice1/dw/dwd/dwd_afanti_travelinfo_list"val dir ="test.txt"
    val dir ="file:///Users/andvoid.lei/test.txt"
    //val dir ="hdfs://nameservice1/Users/andvoid.lei/test.txt"



    val file = spark.read.text(dir).rdd


    p(file) // 1
    file.print // 2


  }


}


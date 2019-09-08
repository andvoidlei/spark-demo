package examples

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession


object testRdd {


  /** Usage: HdfsTest [file] */
  def main(args: Array[String]): Unit = {

    val dv: Vector = Vectors.dense(2.0, 0.0, 8.0)
    val sv2: Vector = Vectors.sparse(3, Seq((0, 2.0), (2, 8.0)))


    val spark = SparkSession
      .builder
      .master("local")
      .appName("HdfsTest")
      .getOrCreate()

    val textFile = spark.sparkContext.textFile("file:///Users/andvoid.lei/dev/spark-2.3.3/README.md")
    //textFile.foreach(println)
    textFile.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println)//.saveAsTextFile("/spark/out")



    val array = Array(1,2,3,4,5)
    val rdd = spark.sparkContext.makeRDD(array)
    val cnt = rdd.count()
    System.out.println(cnt)

  }

}

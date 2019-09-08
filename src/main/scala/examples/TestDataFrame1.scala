package examples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


case class People(var name:String,var age:Int)


object TestDataFrame1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDToDataFrame").setMaster("local")
    val sc = new SparkContext(conf)
    val context = new SQLContext(sc)
    // 将本地的数据读入 RDD， 并将 RDD 与 case class 关联
    //val dir="hdfs://nameservice1/dw/dwd/dwd_afanti_travelinfo_list"
    val dir ="file:///Users/andvoid.lei/test.txt"
    val peopleRDD = sc.textFile(dir)
      .map(line => People(line.split(",")(0), line.split(",")(1).trim.toInt))
    import context.implicits._
    // 将RDD 转换成 DataFrames
    val df = peopleRDD.toDF
    //将DataFrames创建成一个临时的视图

    df.createOrReplaceTempView("people")
    //使用SQL语句进行查询
    context.sql("select * from people").show()
  }

}

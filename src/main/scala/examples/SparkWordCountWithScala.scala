package examples



import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCountWithScala {

  def execBySparkContext(str:String):Unit={

    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)


    //读取文件 生成RDD
    //val file: RDD[String] = sc.textFile("E:\\hello.txt")
    var wordCounts = str.split(" ")//.map(arr => (arr))
    var dataRdd = sc.parallelize(wordCounts)
    //把每一行数据  分割字符
    val word: RDD[String] = dataRdd.flatMap(_.split(","))
    word.foreach(println)
    //让每一个单词都出现一次
    val wordOne: RDD[(String, Int)] = word.map((_, 1))
    //单词计数
    val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_ + _)
    println("单词计数")
    wordCount.foreach(println)
    //按照单词出现的次数 降序排序
    val sortRdd: RDD[(String, Int)] = wordCount.sortBy(tuple => tuple._2, false)
    //将最终的结果进行保存
    //sortRdd.saveAsTextFile("E:\\result")
    sortRdd.foreach(println)

    sc.stop()

  }


  def execBySparkSession(str:String):Unit={

    val spark = SparkSession
      .builder
      .master("local")
      .appName("HdfsTest")
      .getOrCreate()


    //读取文件 生成RDD
    //val file: RDD[String] = sc.textFile("E:\\hello.txt")
    var wordCounts = str.split(" ")//.map(arr => (arr))
    var dataRdd = spark.sparkContext.parallelize(wordCounts)
    //把每一行数据  分割字符
    val word: RDD[String] = dataRdd.flatMap(_.split(","))
    word.foreach(println)
    //让每一个单词都出现一次
    val wordOne: RDD[(String, Int)] = word.map((_, 1))
    //单词计数
    val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_ + _)
    println("单词计数")
    wordCount.foreach(println)
    //按照单词出现的次数 降序排序
    val sortRdd: RDD[(String, Int)] = wordCount.sortBy(tuple => tuple._2, false)
    //将最终的结果进行保存
    //sortRdd.saveAsTextFile("E:\\result")
    sortRdd.foreach(println)

    spark.stop()

  }



  def main(args: Array[String]): Unit = {

    execBySparkContext("hello,e world,1 hello")
    execBySparkSession("hello,e world,1 hello")


  }
}
package examples.fpg


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object fpgTest{
  def p(rdd: org.apache.spark.rdd.RDD[_]) = rdd.foreach(println)


  def main(args: Array[String]): Unit = {

    // **************************  初始化Spark  **************************
    val conf = new SparkConf().setAppName("DistributorProductRecommend")
      .registerKryoClasses(
        Array(classOf[ArrayBuffer[String]], classOf[ListBuffer[String]])
      ).setMaster("local")
    val warehouseLocation = "/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      .config(conf)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    // **************************  直接赋值方法传参  **************************
    val sourceTable = "select ids from dws.dws_rec_user_view_input_fpgrowth_90day where par_day='20171111' limit 10000"  //数据路径
    //    val sourceTable = " select products from dws.dws_tnt_recommend_input_distributor_product_basket where par_day='20180722' limit 1000"
    val targetTable = "dml.dml_rec_distributor_product_fpgrowth_recommend" //hive目标表
    val minSupport = 2.toDouble // 设置最小支持度
    val minConfidence = 0.0001 // 设置最小置信度
    val numPartitions = 5 // 设置分区数

    // **************************  Program arguments 传参  **************************
    //val sourceTable = args(0)  //数据路径
    //val targetTable = args(1) //hive目标表
    //val minSupport = args(2).toDouble //设置最小支持度
    //val minConfidence = args(3).toDouble //设置最小置信度
    //val numPartitions = args(4).toInt //设置分区数

    // **************************  读取数据  **************************
    //val data = spark.sql(sourceTable).rdd  //  通过hive读取数据并转换成RDD格式
    val dir ="file:///Users/andvoid.lei/workSpace/spark-demo/data/fpg_data.csv"
    //val dir ="hdfs://nameservice1/Users/andvoid.lei/test.txt"
    val data = spark.read.text(dir).rdd
    //p(data)
    // **************************  处理购物篮、分隔字符  **************************
    val transaction = data.map{f =>
      val lines = f.getString(0).trim.split(",")//.padTo(6,"-1")
      lines
    }.cache()  //  cache(): RDD写入内存


    // **************************  fpg模型训练  **************************
    // 统计总量
    val count = transaction.count()
    print("count:"+count)
    //设置参数，setMinSupport设置最小支持度，setNumPartitions设置分区数
    val minSup = (minSupport/count).formatted("%.5f").toDouble
    val fpg = new FPGrowth().setMinSupport(minSup).setNumPartitions(numPartitions)
    //模型训练
    val model = fpg.run(transaction)

    // **************************  关联规则提取  **************************
    val suppMap = new mutable.HashMap[String,Double]() //接收频繁项，计算概率值，用于计算提升度
    model.freqItemsets.collect()
      .filter(f => f.items.length == 1)
      .foreach{s =>
        val item = s.items.mkString(",")
        val sup = s.freq.toDouble/count
        suppMap.+=((item,sup))
      }  // 计算单个item的支持度

    //  获得关联规则，并利用最小置信度对关联规则进行过滤
    val associationRules = model.generateAssociationRules(minConfidence)

    val filterRules = associationRules
      .filter(f => f.antecedent.length  == 1)
      .repartition(1)
      .map{s =>
        val supp = suppMap.get(s.consequent.mkString(","))  //  s.consequent为后导项，从哈希表中获取对应的后置项的支持度。
      val lift = s.confidence/supp.get                    //  此处的提升度并非完全按照公式计算，

        (s.antecedent.mkString(","),s.consequent.mkString(","),s.confidence,lift)
        //Returns antecedent in a Java List.  // 前导项，相当于第一个product_id
        //Returns consequent in a Java List.  // 后置项，相当于第二个product_id
      }
      .filter(_._4 > 1L)
      .groupBy(_._1)
      .sortByKey(false)
      .flatMap(m=>m._2.toList.sortWith(_._3>_._3))

    import spark.implicits._
    val resultDF = filterRules.toDF()
    resultDF.show()

    sc.stop()
    spark.stop()
    println("\nI am ok !!!")
  }
}

/*
结果样本
（前导productId，后置productId，置信度，支持度）
count:21+---+---+-------------------+------------------+
| _1| _2|                 _3|                _4|
+---+---+-------------------+------------------+
|  z|  x|                0.6|              3.15|
|  z|  y|                0.6|               2.1|
|  z|  t|                0.6|               1.8|
|  y|  t| 0.6666666666666666|               2.0|
|  y|  x|                0.5|             2.625|
|  y|  s|                0.5|               1.5|
|  y|  z|                0.5|               2.1|
|  x|  y|               0.75|             2.625|
|  x|  z|               0.75|3.1500000000000004|
|  x|  t|               0.75|              2.25|
|  x|  s|               0.75|              2.25|
|  w|  s|                1.0|               3.0|
|  v|  t|               0.75|              2.25|
|  t|  y| 0.5714285714285714|               2.0|
|  t|  x|0.42857142857142855|              2.25|
|  t|  s|0.42857142857142855|1.2857142857142858|
|  t|  v|0.42857142857142855|              2.25|
|  t|  z|0.42857142857142855|               1.8|
|  s|  x|0.42857142857142855|              2.25|
|  s|  w|0.42857142857142855|               3.0|
+---+---+-------------------+------------------+
*/

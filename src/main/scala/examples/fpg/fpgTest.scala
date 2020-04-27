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
（前导Id，后置Id，置信度，支持度）
count:19+---+---+------------------+------------------+
| _1| _2|                _3|                _4|
+---+---+------------------+------------------+
|  z|  x|               0.6|              2.85|
|  z|  y|               0.6|1.9000000000000001|
|  z|  t|               0.6|1.6285714285714286|
|  z|  p|               0.4|3.8000000000000003|
|  z|  r|               0.4|1.2666666666666668|
|  z|  s|               0.4|1.0857142857142859|
|  z|  q|               0.4|2.5333333333333337|
|  y|  t|0.6666666666666666|1.8095238095238095|
|  y|  x|               0.5|             2.375|
|  y|  s|               0.5|1.3571428571428572|
|  y|  z|               0.5|1.9000000000000001|
|  y|  w|0.3333333333333333| 2.111111111111111|
|  y|  e|0.3333333333333333|1.5833333333333333|
|  y|  q|0.3333333333333333| 2.111111111111111|
|  x|  y|              0.75|             2.375|
|  x|  z|              0.75|              2.85|
|  x|  t|              0.75| 2.035714285714286|
|  x|  s|              0.75| 2.035714285714286|
|  x|  r|               0.5|1.5833333333333335|
|  x|  q|               0.5| 3.166666666666667|
+---+---+------------------+------------------+
*/

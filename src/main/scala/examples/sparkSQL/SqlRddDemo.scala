package examples.sparkSQL

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SqlRddDemo {


  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder().appName("JoinTest").master("local[*]").getOrCreate()
    //创建数据
    import session.implicits._
    val table1: Dataset[String] = session.createDataset(List("1,xiaoli,china", "2,xiaohua,usa", "3,xiaming,England"))
    //对数据进行整理
    val Dept: Dataset[(Long, String, String)] = table1.map(line => {
      val word: Array[String] = line.split(",")
      val id = word(0).toLong
      val name = word(1).toString
      val country = word(2).toString
      (id, name, country)
    })

    val table2: Dataset[String] = session.createDataset(List("china,中国", "usa,美国"))
    //对数据进行整理
    val Dept1: Dataset[(String, String)] = table2.map(line => {
      val word = line.split(",")
      val nation = word(0).toString
      val nation1 = word(1).toString
      (nation, nation1)
    })

    //转换成DataFrame
    val df: DataFrame = Dept.toDF("id", "name", "country")
    val df1: DataFrame = Dept1.toDF("nation", "nation1")

    //第一种，创建视图(表)
    //    df.createTempView("table1")
    //    df1.createTempView("table2")
    //
    //    val ys: DataFrame = session.sql("SELECT name,nation1 FROM table1 JOIN table2 ON country=nation")


    //第二种，DataFrame API
    val ys: DataFrame = df.join(df1, $"country" === $"nation", "left")


    ys.show()

    session.stop()

  }
}

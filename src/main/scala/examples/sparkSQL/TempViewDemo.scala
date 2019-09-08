package examples.sparkSQL

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object TempViewDemo {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("JoinDemo").master("local[*]").getOrCreate()

    import spark.implicits._
    val subjectData: Dataset[String] = spark.createDataset(List("1,math,98", "2,math,60", "3,math,50"))

    val sb: Dataset[(Int, String, Int)] = subjectData.map(l => {
      val fields = l.split(",")
      val id = fields(0).toInt
      val subject = fields(1)
      val score = fields(2).toInt
      (id, subject, score)
    })

    val studentData: Dataset[String] = spark.createDataset(List("1,大宝", "2,渣渣", "3,55"))

    val student: Dataset[(Int, String)] = studentData.map(l => {
      val fields = l.split(",")
      val id = fields(0).toInt
      val name = fields(1)
      (id, name)
    })

    val stDataFrame: DataFrame = student.toDF("sid", "name")
    val sbDataFrame = sb.toDF("uid", "math", "score")

    //dataFrame方式
    //val result = sbDataFrame.join(stDataFrame, $"uid" === $"sid", "right")

    //spark sql方式
    sbDataFrame.createTempView("t_sb")
    stDataFrame.createTempView("t_student")
    val result = spark.sql(" select t.* , s.name from t_sb t join t_student s on  t.uid = s.sid")

    result.show()
    //    +---+----+-----+----+
    //    |uid|math|score|name|
    //    +---+----+-----+----+
    //    |  1|math|   98|  大宝|
    //    |  2|math|   60|  渣渣|
    //    |  3|math|   50|  55|
    //    +---+----+-----+----+

    spark.stop()
  }

}

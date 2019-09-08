package examples.sparkSQL

import org.apache.spark.sql.SparkSession

object JoinExample {


  def main(args: Array[String]) {
    // $example on:init_session$
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._
    val df = spark.createDataset(Seq(("tom", 21, 19), ("jerry", 31, 19), ("jack", 32, 18)))
      .toDF("name", "age", "salary")
    val df1 = spark.createDataset(Seq(("tom", "a"), ("jerry", "b"), ("tony", "d")))
      .toDF("name1", "grade")

    df.show()
    //    +-----+---+------+
    //    | name|age|salary|
    //    +-----+---+------+
    //    |  tom| 21|    19|
    //    |jerry| 31|    19|
    //    | jack| 32|    18|
    //    +-----+---+------+
    df1.show()
    //    +-----+-----+
    //    |name1|grade|
    //    +-----+-----+
    //    |  tom|    a|
    //    |jerry|    b|
    //    | tony|    d|
    //    +-----+-----+

    df.join(df1).where($"name" === $"name1").show()
    //    +-----+---+------+-----+-----+
    //    | name|age|salary|name1|grade|
    //    +-----+---+------+-----+-----+
    //    |  tom| 21|    19|  tom|    a|
    //    |jerry| 31|    19|jerry|    b|
    //    +-----+---+------+-----+-----+

    df.join(df1, df("name") === df1("name1")).where($"name" === $"name1").show()
    //    +-----+---+------+-----+-----+
    //    | name|age|salary|name1|grade|
    //    +-----+---+------+-----+-----+
    //    |  tom| 21|    19|  tom|    a|
    //    |jerry| 31|    19|jerry|    b|
    //    +-----+---+------+-----+-----+
    //可以将连接的字段名，改成一样的，连接的字段只有一列，如下图第二张
    val df2 = df1.withColumnRenamed("name1", "name")
    df.join(df2, "name").show()
    //    +-----+---+------+-----+
    //    | name|age|salary|grade|
    //    +-----+---+------+-----+
    //    |  tom| 21|    19|    a|
    //    |jerry| 31|    19|    b|
    //    +-----+---+------+-----+

    df.join(df2, Seq("name")).show()
    //    +-----+---+------+-----+
    //    | name|age|salary|grade|
    //    +-----+---+------+-----+
    //    |  tom| 21|    19|    a|
    //    |jerry| 31|    19|    b|
    //    +-----+---+------+-----+

    df.join(df2, Seq("name"), "outer").show()
    //    +-----+----+------+-----+
    //    | name| age|salary|grade|
    //    +-----+----+------+-----+
    //    | jack|  32|    18| null|
    //    |jerry|  31|    19|    b|
    //    | tony|null|  null|    d|
    //    |  tom|  21|    19|    a|
    //    +-----+----+------+-----+


    df.join(df2, Seq("name"), "left").show()
    //    +-----+---+------+-----+
    //    | name|age|salary|grade|
    //    +-----+---+------+-----+
    //    |  tom| 21|    19|    a|
    //    |jerry| 31|    19|    b|
    //    | jack| 32|    18| null|
    //    +-----+---+------+-----+


    df.join(df1, df("name") === df1("name1"), "left").show()
    //    +-----+---+------+-----+-----+
    //    | name|age|salary|name1|grade|
    //    +-----+---+------+-----+-----+
    //    |  tom| 21|    19|  tom|    a|
    //    |jerry| 31|    19|jerry|    b|
    //    | jack| 32|    18| null| null|
    //    +-----+---+------+-----+-----+


    df.join(df2, Seq("name"), "right").show()
    //    +-----+----+------+-----+
    //    | name| age|salary|grade|
    //    +-----+----+------+-----+
    //    |  tom|  21|    19|    a|
    //    |jerry|  31|    19|    b|
    //    | tony|null|  null|    d|
    //    +-----+----+------+-----+


    df.join(df2, Seq("name"), "leftsemi").show()
    //select * from df where name in (select name from df2);
    //    +-----+---+------+
    //    | name|age|salary|
    //    +-----+---+------+
    //    |  tom| 21|    19|
    //    |jerry| 31|    19|
    //    +-----+---+------+
    df.join(df2, Seq("name"), "leftanti").show()
    //select * from df where name not in (select name from df2);
    //    +----+---+------+
    //    |name|age|salary|
    //    +----+---+------+
    //    |jack| 32|    18|
    //    +----+---+------+

    df.crossJoin(df2).show()
    //    +-----+---+------+-----+-----+
    //    | name|age|salary| name|grade|
    //    +-----+---+------+-----+-----+
    //    |  tom| 21|    19|  tom|    a|
    //    |jerry| 31|    19|  tom|    a|
    //    | jack| 32|    18|  tom|    a|
    //    |  tom| 21|    19|jerry|    b|
    //    |jerry| 31|    19|jerry|    b|
    //    | jack| 32|    18|jerry|    b|
    //    |  tom| 21|    19| tony|    d|
    //    |jerry| 31|    19| tony|    d|
    //    | jack| 32|    18| tony|    d|
    //    +-----+---+------+-----+-----+

    spark.stop()


  }


}

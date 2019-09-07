package examples


import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source


/**
  * Created by xiaojun on 2015/10/19.
  */
object TfIdfDemo {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("TfIdfTest").setMaster("local")
    val sc = new SparkContext(conf)

    // Load documents (one per line).要求每行作为一个document,这里zipWithIndex将每一行的行号作为doc id
    val documents = sc.parallelize(Source.fromFile("CHANGELOG").getLines().filter(_.trim.length > 0).toSeq).map(_.split(" ").toSeq).zipWithIndex()


    val hashingTF = new HashingTF(Math.pow(2, 18).toInt)
    //这里将每一行的行号作为doc id，每一行的分词结果生成tf词频向量
    val tf_num_pairs = documents.map {
      case (seq, num) =>
        val tf = hashingTF.transform(seq)
        (num, tf)
    }
    tf_num_pairs.cache()
    //构建idf model
    val idf = new IDF().fit(tf_num_pairs.values)
    //将tf向量转换成tf-idf向量
    val num_idf_pairs = tf_num_pairs.mapValues(v => idf.transform(v))
    //广播一份tf-idf向量集
    val b_num_idf_pairs = sc.broadcast(num_idf_pairs.collect())

    //计算doc之间余弦相似度
    val docSims = num_idf_pairs.flatMap {
      case (id1, idf1) =>
        val idfs = b_num_idf_pairs.value.filter(_._1 != id1)
        val sv1 = idf1.asInstanceOf[SV]
        import breeze.linalg._
        val bsv1 = new SparseVector[Double](sv1.indices, sv1.values, sv1.size)
        idfs.map {
          case (id2, idf2) =>
            val sv2 = idf2.asInstanceOf[SV]
            val bsv2 = new SparseVector[Double](sv2.indices, sv2.values, sv2.size)
            val cosSim = bsv1.dot(bsv2).asInstanceOf[Double] / (norm(bsv1) * norm(bsv2))
            (id1, id2, cosSim)
        }
    }
    docSims.foreach(println)

    sc.stop()

  }

}

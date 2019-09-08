package examples



import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by Administrator on 2017/7/6.
  */
object DecisionTreeTest {

  def main(args: Array[String]): Unit = {

    // 设置运行环境
    val conf = new SparkConf().setAppName("Decision Tree")
      .setMaster("local")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    // 读取样本数据并解析
    val dataRDD = MLUtils.loadLibSVMFile(sc, "./data/sample_libsvm_data.txt")
    // 样本数据划分,训练样本占0.8,测试样本占0.2
    val dataParts = dataRDD.randomSplit(Array(0.8, 0.2))
    val trainRDD = dataParts(0)
    val testRDD = dataParts(1)

    // 决策树参数
    val numClasses = 5
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32
    // 建立决策树模型并训练
    val model = DecisionTree.trainClassifier(trainRDD, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // 对测试样本进行测试
    val predictionAndLabel = testRDD.map { point =>
      val score = model.predict(point.features)
      (score, point.label, point.features)
    }
    val showPredict = predictionAndLabel.take(50)
    println("Prediction" + "\t" + "Label" + "\t" + "Data")
    for (i <- 0 to showPredict.length - 1) {
      println(showPredict(i)._1 + "\t" + showPredict(i)._2 + "\t" + showPredict(i)._3)
    }

    // 误差计算
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testRDD.count()
    println("Accuracy = " + accuracy)

    // 保存模型
    val ModelPath = "./data/model/Decision_Tree_Model"
    model.save(sc, ModelPath)
    val sameModel = DecisionTreeModel.load(sc, ModelPath)

  }
}
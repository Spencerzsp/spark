package com.fsnip.spark.ml.myml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, IDF, LabeledPoint, Tokenizer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 14:48 2019/7/17
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object TextClassification {

  case class RawDataRecord(category: String, text: String)
  def main(args: Array[String]): Unit = {
    val spark= SparkSession
      .builder()
      .appName("TextClassify")
      .master("local[*]")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val sc = spark.sparkContext

    import spark.implicits._

    val srcData = sc.textFile("/user/test1/sougou-train")
      .map{ line => {
        val data = line.split(",")
        RawDataRecord(data(0), data(1))
      }}.toDF("category", "text")

//    srcData.collect().foreach(println(_))
//    srcData.take(1).foreach(println)

    val Array(trainingData, testData) = srcData.randomSplit(Array(0.7, 0.3))

//    将词语转换成数组
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val wordsData = tokenizer.transform(trainingData)

    println("output1: ")
    wordsData.select("category", "text", "words").take(1).foreach(println)

    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(1000)
    val featurizedData = hashingTF.transform(wordsData)

    println("output2: ")
    featurizedData.select("category", "words", "rawFeatures").take(1).foreach(println)

    val idf = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)

    println("output3: ")
    rescaledData.select("category", "features").take(1).foreach(println)

//    转换成贝叶斯的输入格式
    val trainDataRDD = rescaledData.select("category", "features").map{
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }

    println("output4: ")
    rescaledData.select("category", "features").take(1).foreach(println)

//    训练贝叶斯模型
//    val modelBayes = NaiveBayes.train(trainDataRDD.rdd, lambda = 1.0, modelType = "multinomial")
    val modelBayes = new NaiveBayes().fit(trainDataRDD)

//    测试数据集，做同样的特征表示以及格式转换
    val testWordsData = tokenizer.transform(testData)
    val testFeaturedData = hashingTF.transform(testWordsData)
    val testRescaledData = idfModel.transform(testFeaturedData)
    val testDataRDD = testRescaledData.select("category", "features").map{
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }

//    val testPredictionAndLabel = testDataRDD.map(p =>(modelBayes.predict(p.features), p.label))
//    val predictions = modelBayes.transform(testDataRDD)
//    predictions.select("prediction", "rawPrediction", "probability").show(1, false)
//    统计分类准确率
//    var accuracy = 1.0 * testPredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRDD.count()
    println("output5: ")
//    println(accuracy)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
//    val accuracy2 = evaluator.evaluate(predictions)

    println("output6: ")
//    println(accuracy2)

//    val modelPath = "file:///F:\\scala-workspace\\spark\\data\\model\\naive_bayes_model"
//    modelBayes.save(modelPath)

//    val sameModel = NaiveBayesModel.load(sc, modelPath)
//    println(sameModel.labels)
//    println(sameModel.pi)
//    println(sameModel.theta)
//    println(sameModel.modelType)
  }

}

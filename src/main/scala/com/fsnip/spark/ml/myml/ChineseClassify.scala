package com.fsnip.spark.ml.myml


import org.apache.spark.ml.Pipeline
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{HashingTF, IDF, LabeledPoint, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 10:43 2019/7/17
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object ChineseClassify {

  case class RawDataRecord(category: String, text: String)
  case class RawDataRecord2(text: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ChineseClassify")
      .master("local[*]")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val sc = spark.sparkContext
    import spark.implicits._
    val training = sc.textFile("file:///F:\\scala-workspace\\spark\\data\\sougou-train\\C000007.txt").map(line => {
      val data = line.split("\t")
      RawDataRecord(data(0), data(1))
    }).toDF("category", "text")

//    training.show(false)
//    training.select("category", "text").take(2).foreach(println(_))

    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
//    val wordsData = tokenizer.transform(training)
//    wordsData.select("category","text", "words").take(2).foreach(println(_))

    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(100)

    val idf = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")

    val srcDF2 = sc.textFile("file:///F:\\scala-workspace\\spark\\data\\sougou-train\\C000008.txt")
      .map(RawDataRecord2(_)).toDF("text")

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, idf))

    val idmodel = pipeline.fit(training)
    val rescaleData = idmodel.transform(training)

    val trainDataRDD = rescaleData.select("categoty", "features").map{
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }
    val model = new NaiveBayes().fit(trainDataRDD)

//    测试数据，做同样的格式转化
    val testRescaledData = idmodel.transform(srcDF2)
    val testDataRDD = testRescaledData.select("features").map{
      case Row(features: Vector) =>
        LabeledPoint(0.0, Vectors.dense(features.toArray))
    }

    val testpredictionAndLabel = model.transform(testDataRDD)
    testpredictionAndLabel.show(false)


  }

}

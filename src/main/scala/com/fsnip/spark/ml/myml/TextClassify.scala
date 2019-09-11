package com.fsnip.spark.ml.myml

import com.fsnip.spark.ml.myml.ChineseClassify.RawDataRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 13:50 2019/7/17
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object TextClassify {

  def main(args: Array[String]): Unit = {
    val spark= SparkSession
      .builder()
      .appName("TextClassify")
      .master("local[*]")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val sc = spark.sparkContext

    import spark.implicits._
    val srcDF = sc.textFile("/user/test1/output/js")
      .map(line => {
        val data = line.split(",")
        RawDataRecord(data(0), data(1))
      }).toDF("category", "text")

    srcDF.show(false)

    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")

    val wordsdData = tokenizer.transform(srcDF)
    wordsdData.select($"category", $"text", $"words").show(false)

    val featuredData = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(100)

    val hashingTFedData = featuredData.transform(wordsdData)
    hashingTFedData.take(2).foreach(println(_))

    val idf = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")

    val idfModel = idf.fit(hashingTFedData)

    val rescaledData = idfModel.transform(hashingTFedData)
    rescaledData.select("category", "words", "features").take(2).foreach(println)

//    将数据转换成Bayes算法需要的格式

    val trainDataRDD = rescaledData.select($"category", $"features")
      .map { case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
      }

    trainDataRDD.rdd.take(1).foreach(println)

//    val Array(trainingData, testData) = trainDataRDD.randomSplit(Array(0.7,0.3), seed = 1234L)
//
//    val modelBayes = new NaiveBayes().fit(trainingData)
//    val predictions = modelBayes.transform(testData)
//    predictions.show(false)

//    val modelBayes = NaiveBayes.train(trainDataRDD.rdd, lambda = 1.0, modelType = "multinomial")
  }
}

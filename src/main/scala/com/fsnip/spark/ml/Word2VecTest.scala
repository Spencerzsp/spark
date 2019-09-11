package com.fsnip.spark.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 9:49 2019/1/23
  * @ Description：Word2vec是一个Estimator，它采用一系列代表文档的词语来训练word2vecmodel。
  * 该模型将每个词语映射到一个固定大小的向量。word2vecmodel使用文档中每个词语的平均数来将文档转换为向量，然后这个向量可以作为预测的特征，来计算文档相似度计算等等
  * @ Modified By：
  * @ Version:     
  */
object Word2VecTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Word2Vec").master("local[*]").getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val documentDF = spark.createDataFrame(Seq(

      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(5)
      .setMinCount(0)

    val model = word2Vec.fit(documentDF)

    val result = model.transform(documentDF)

    result.collect().foreach{case Row(text: Seq[_], features: Vector) =>
    println(s"Text: [${text.mkString(",")}] => \nVector: $features\n")}

    result.select("result").take(5).foreach(println(_))
  }
}

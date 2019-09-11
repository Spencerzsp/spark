package com.fsnip.spark.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 17:46 2019/1/22
  * @ Description：pipline测试
  * @ Modified By：
  * @ Version:     
  */
object PipLine {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("PipLine").master("local[*]").getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val training = spark.createDataFrame(Seq(

      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)

    )).toDF("id", "text", "label")

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")

    val hashingTF = new HashingTF()
      .setNumFeatures(100)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.01)

    val pipline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))

    val model = pipline.fit(training)

    model.write.overwrite().save("file:///./spark-logistic-regression-model")

    pipline.write.overwrite().save("file:///./unfit-lr-model")

    val sameModel = PipelineModel.load("file:///./spark-logistic-regression-model")

    val test = spark.createDataFrame(Seq(

      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "mapreduce spark"),
      (7L, "apache hadoop")

    )).toDF("id", "text")

    model.transform(test)
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach{ case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")

      }
  }

}

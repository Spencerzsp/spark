package com.fsnip.spark.ml.myml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 13:49 2019/7/19
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object ALSExample {

  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
  def main(args: Array[String]): Unit = {
    val spark= SparkSession
      .builder()
      .appName("TextClassify")
      .master("local[*]")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)
    import spark.implicits._

    val dataset = spark.read.textFile("file:///F:\\spark-2.3.0-bin-hadoop2.7\\data\\mllib\\als\\sample_movielens_ratings.txt")
    val ratings = dataset.map{
      x => {
        val fields = x.split("::")
        assert(fields.size == 4)
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
      }
    }.toDF()
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)

    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("rating")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

  }

}

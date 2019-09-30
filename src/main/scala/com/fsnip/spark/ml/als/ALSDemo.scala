package com.fsnip.spark.ml.als

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 10:34 2019/9/29
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
case class Movie(movieId: Int, title: String, genres: Seq[String])

case class User(userId: Int, gender: String, age: Int, occupation: Int, zip: String)

object ALSDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ALSDemo")
      .master("local[2]")
      .getOrCreate()

    val ratingText = spark.sparkContext.textFile("file:///F:\\spark-2.3.0-bin-hadoop2.7\\data\\mllib\\als\\sample_movielens_ratings.txt")

    println(ratingText.first())
    val ratingRDD = ratingText.map(parseRating).cache()
    println("Total number of datings:" + ratingRDD.count())
    println("Total number of movies rated: " + ratingRDD.map(_.product).distinct().count())
    println("Total number of users rated: " + ratingRDD.map(_.user).distinct().count())

    import spark.sqlContext.implicits._
    val ratingDF = ratingRDD.toDF()
//    val ratingDF = spark.read.text("file:///F:\\spark-2.3.0-bin-hadoop2.7\\data\\mllib\\als\\sample_movielens_ratings.txt")
    println(ratingDF.schema)
  }

  def parseMovie(str:String): Movie ={
    val fields = str.split("::")
    assert(fields.size == 3)
    Movie(fields(0).toInt, fields(1), Seq(fields(2)))
  }

  def parseUser(str:String): User ={
    val fields = str.split("::")
    assert(fields.size ==5)
    User(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt, fields(4))
  }

  def parseRating(str:String): Rating ={
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toInt)
  }

}

package com.fsnip.spark.ml

import org.apache.spark.ml.linalg.{Vectors,Vector}
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 9:17 2019/5/22
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object SummarizerTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SummarizerTest")
      .master("local[*]")
      .getOrCreate()

    val data = Seq(
      (Vectors.dense(2.0, 3.0, 5.0), 1.0),
      (Vectors.dense(4.0, 6.0, 7.0), 2.0)
    )

    import spark.implicits._
    val df = data.toDF("features", "weight")
    df.show()

    import Summarizer._
    val (meanVal, varianceVal) = df.select(metrics("mean", "variance")
      .summary($"features", $"weight").as("summary"))
      .select("summary.mean", "summary.variance")
      .as[(Vector, Vector)].first()

    println(s"with weight: mean = ${meanVal}, variance = ${varianceVal}")

  }

}

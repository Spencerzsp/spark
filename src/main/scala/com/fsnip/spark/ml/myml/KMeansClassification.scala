package com.fsnip.spark.ml.myml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 13:29 2019/7/19
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object KMeansClassification {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("KMeansClassification")
      .master("local[2]")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    import spark.implicits._

    val data = spark.read.format("libsvm").load("file:///F:\\spark-2.3.0-bin-hadoop2.7\\data\\mllib\\sample_kmeans_data.txt")

    val kmeans = new KMeans()
      .setK(2)
      .setSeed(1L)
    val model = kmeans.fit(data)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(data)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

  }
}

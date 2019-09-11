package com.fsnip.spark.ml

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 16:16 2019/1/25
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object KMeansExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("KMeans")
      .master("local[*]")
      .getOrCreate()

    val dataset = spark.read.format("libsvm")
      .load("F:/spark-2.3.0-bin-hadoop2.7/data/mllib/sample_kmeans_data.txt")

//    dataset.createOrReplaceTempView("data")
//    spark.newSession().sql("")

    val kmeans = new KMeans().setK(2).setSeed(1L)

    val model = kmeans.fit(dataset)

    val predictions = model.transform(dataset)

    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)

    println(s"Silhouette with squared euclidean distance = $silhouette")

    println("Cluster Centers: ")

    model.clusterCenters.foreach(println)
  }

}

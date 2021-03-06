package com.fsnip.spark.ml.myml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 15:52 2019/7/18
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object DecisionTreeClasscification {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DecisionTreeClasscification")
      .master("local[2]")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val dataList: List[(Double, String, Double, Double, String, Double, Double, Double, Double)] = List(
      (0, "male", 37, 10, "no", 3, 18, 7, 4),
      (0, "female", 27, 4, "no", 4, 14, 6, 4),
      (0, "female", 32, 15, "yes", 1, 12, 1, 4),
      (0, "male", 57, 15, "yes", 5, 18, 6, 5),
      (0, "male", 22, 0.75, "no", 2, 17, 6, 3),
      (0, "female", 32, 1.5, "no", 2, 17, 5, 5),
      (0, "female", 22, 0.75, "no", 2, 12, 1, 3),
      (0, "male", 57, 15, "yes", 2, 14, 4, 4),
      (0, "female", 32, 15, "yes", 4, 16, 1, 2),
      (0, "male", 22, 1.5, "no", 4, 14, 4, 5),
      (0, "male", 37, 15, "yes", 2, 20, 7, 2),
      (0, "male", 27, 4, "yes", 4, 18, 6, 4),
      (0, "male", 47, 15, "yes", 5, 17, 6, 4),
      (0, "female", 22, 1.5, "no", 2, 17, 5, 4),
      (0, "female", 27, 4, "no", 4, 14, 5, 4),
      (0, "female", 37, 15, "yes", 1, 17, 5, 5),
      (0, "female", 37, 15, "yes", 2, 18, 4, 3),
      (0, "female", 22, 0.75, "no", 3, 16, 5, 4),
      (0, "female", 22, 1.5, "no", 2, 16, 5, 5),
      (0, "female", 27, 10, "yes", 2, 14, 1, 5),
      (0, "female", 22, 1.5, "no", 2, 16, 5, 5),
      (0, "female", 22, 1.5, "no", 2, 16, 5, 5),
      (0, "female", 27, 10, "yes", 4, 16, 5, 4),
      (0, "female", 32, 10, "yes", 3, 14, 1, 5),
      (0, "male", 37, 4, "yes", 2, 20, 6, 4),
      (0, "female", 22, 1.5, "no", 2, 18, 5, 5),
      (0, "female", 27, 7, "no", 4, 16, 1, 5),
      (0, "male", 42, 15, "yes", 5, 20, 6, 4),
      (0, "male", 27, 4, "yes", 3, 16, 5, 5),
      (0, "female", 27, 4, "yes", 3, 17, 5, 4),
      (0, "male", 42, 15, "yes", 4, 20, 6, 3),
      (0, "female", 22, 1.5, "no", 3, 16, 5, 5),
      (0, "male", 27, 0.417, "no", 4, 17, 6, 4),
      (0, "female", 42, 15, "yes", 5, 14, 5, 4),
      (0, "male", 32, 4, "yes", 1, 18, 6, 4),
      (0, "female", 22, 1.5, "no", 4, 16, 5, 3),
      (0, "female", 42, 15, "yes", 3, 12, 1, 4),
      (0, "female", 22, 4, "no", 4, 17, 5, 5),
      (0, "male", 22, 1.5, "yes", 1, 14, 3, 5),
      (0, "female", 22, 0.75, "no", 3, 16, 1, 5),
      (0, "male", 32, 10, "yes", 5, 20, 6, 5),
      (0, "male", 52, 15, "yes", 5, 18, 6, 3),
      (0, "female", 22, 0.417, "no", 5, 14, 1, 4),
      (0, "female", 27, 4, "yes", 2, 18, 6, 1),
      (0, "female", 32, 7, "yes", 5, 17, 5, 3),
      (0, "male", 22, 4, "no", 3, 16, 5, 5),
      (0, "female", 27, 7, "yes", 4, 18, 6, 5),
      (0, "female", 42, 15, "yes", 2, 18, 5, 4),
      (0, "male", 27, 1.5, "yes", 4, 16, 3, 5),
      (0, "male", 42, 15, "yes", 2, 20, 6, 4),
      (0, "female", 22, 0.75, "no", 5, 14, 3, 5),
      (0, "male", 32, 7, "yes", 2, 20, 6, 4),
      (0, "male", 27, 4, "yes", 5, 20, 6, 5),
      (0, "male", 27, 10, "yes", 4, 20, 6, 4),
      (0, "male", 22, 4, "no", 1, 18, 5, 5),
      (0, "female", 37, 15, "yes", 4, 14, 3, 1),
      (0, "male", 22, 1.5, "yes", 5, 16, 4, 4),
      (0, "female", 37, 15, "yes", 4, 17, 1, 5),
      (0, "female", 27, 0.75, "no", 4, 17, 5, 4),
      (0, "male", 32, 10, "yes", 4, 20, 6, 4),
      (0, "female", 47, 15, "yes", 5, 14, 7, 2),
      (0, "male", 37, 10, "yes", 3, 20, 6, 4),
      (0, "female", 22, 0.75, "no", 2, 16, 5, 5),
      (0, "male", 27, 4, "no", 2, 18, 4, 5),
      (0, "male", 32, 7, "no", 4, 20, 6, 4),
      (0, "male", 42, 15, "yes", 2, 17, 3, 5),
      (0, "male", 37, 10, "yes", 4, 20, 6, 4),
      (0, "female", 47, 15, "yes", 3, 17, 6, 5),
      (0, "female", 22, 1.5, "no", 5, 16, 5, 5),
      (0, "female", 27, 1.5, "no", 2, 16, 6, 4),
      (0, "female", 27, 4, "no", 3, 17, 5, 5),
      (0, "female", 32, 10, "yes", 5, 14, 4, 5),
      (0, "female", 22, 0.125, "no", 2, 12, 5, 5),
      (0, "male", 47, 15, "yes", 4, 14, 4, 3),
      (0, "male", 32, 15, "yes", 1, 14, 5, 5),
      (0, "male", 27, 7, "yes", 4, 16, 5, 5),
      (0, "female", 22, 1.5, "yes", 3, 16, 5, 5),
      (0, "male", 27, 4, "yes", 3, 17, 6, 5),
      (0, "female", 22, 1.5, "no", 3, 16, 5, 5),
      (0, "male", 57, 15, "yes", 2, 14, 7, 2),
      (0, "male", 17.5, 1.5, "yes", 3, 18, 6, 5),
      (0, "male", 57, 15, "yes", 4, 20, 6, 5),
      (0, "female", 22, 0.75, "no", 2, 16, 3, 4),
      (0, "male", 42, 4, "no", 4, 17, 3, 3),
      (0, "female", 22, 1.5, "yes", 4, 12, 1, 5),
      (0, "female", 22, 0.417, "no", 1, 17, 6, 4),
      (0, "female", 32, 15, "yes", 4, 17, 5, 5),
      (0, "female", 27, 1.5, "no", 3, 18, 5, 2),
      (0, "female", 22, 1.5, "yes", 3, 14, 1, 5),
      (0, "female", 37, 15, "yes", 3, 14, 1, 4),
      (0, "female", 32, 15, "yes", 4, 14, 3, 4),
      (0, "male", 37, 10, "yes", 2, 14, 5, 3),
      (0, "male", 37, 10, "yes", 4, 16, 5, 4),
      (0, "male", 57, 15, "yes", 5, 20, 5, 3),
      (0, "male", 27, 0.417, "no", 1, 16, 3, 4),
      (0, "female", 42, 15, "yes", 5, 14, 1, 5),
      (0, "male", 57, 15, "yes", 3, 16, 6, 1),
      (0, "male", 37, 10, "yes", 1, 16, 6, 4),
      (0, "male", 37, 15, "yes", 3, 17, 5, 5),
      (0, "male", 37, 15, "yes", 4, 20, 6, 5),
      (0, "female", 27, 10, "yes", 5, 14, 1, 5),
      (0, "male", 37, 10, "yes", 2, 18, 6, 4),
      (0, "female", 22, 0.125, "no", 4, 12, 4, 5),
      (0, "male", 57, 15, "yes", 5, 20, 6, 5),
      (0, "female", 37, 15, "yes", 4, 18, 6, 4),
      (0, "male", 22, 4, "yes", 4, 14, 6, 4),
      (0, "male", 27, 7, "yes", 4, 18, 5, 4),
      (0, "male", 57, 15, "yes", 4, 20, 5, 4),
      (0, "male", 32, 15, "yes", 3, 14, 6, 3),
      (0, "female", 22, 1.5, "no", 2, 14, 5, 4),
      (0, "female", 32, 7, "yes", 4, 17, 1, 5),
      (0, "female", 37, 15, "yes", 4, 17, 6, 5),
      (0, "female", 32, 1.5, "no", 5, 18, 5, 5),
      (0, "male", 42, 10, "yes", 5, 20, 7, 4),
      (0, "female", 27, 7, "no", 3, 16, 5, 4),
      (0, "male", 37, 15, "no", 4, 20, 6, 5),
      (0, "male", 37, 15, "yes", 4, 14, 3, 2),
      (0, "male", 32, 10, "no", 5, 18, 6, 4),
      (0, "female", 22, 0.75, "no", 4, 16, 1, 5),
      (0, "female", 27, 7, "yes", 4, 12, 2, 4),
      (0, "female", 27, 7, "yes", 2, 16, 2, 5),
      (0, "female", 42, 15, "yes", 5, 18, 5, 4),
      (0, "male", 42, 15, "yes", 4, 17, 5, 3),
      (0, "female", 27, 7, "yes", 2, 16, 1, 2),
      (0, "female", 22, 1.5, "no", 3, 16, 5, 5),
      (0, "male", 37, 15, "yes", 5, 20, 6, 5),
      (0, "female", 22, 0.125, "no", 2, 14, 4, 5),
      (0, "male", 27, 1.5, "no", 4, 16, 5, 5),
      (0, "male", 32, 1.5, "no", 2, 18, 6, 5),
      (0, "male", 27, 1.5, "no", 2, 17, 6, 5),
      (0, "female", 27, 10, "yes", 4, 16, 1, 3),
      (0, "male", 42, 15, "yes", 4, 18, 6, 5),
      (0, "female", 27, 1.5, "no", 2, 16, 6, 5),
      (0, "male", 27, 4, "no", 2, 18, 6, 3),
      (0, "female", 32, 10, "yes", 3, 14, 5, 3),
      (0, "female", 32, 15, "yes", 3, 18, 5, 4),
      (0, "female", 22, 0.75, "no", 2, 18, 6, 5),
      (0, "female", 37, 15, "yes", 2, 16, 1, 4),
      (0, "male", 27, 4, "yes", 4, 20, 5, 5),
      (0, "male", 27, 4, "no", 1, 20, 5, 4),
      (0, "female", 27, 10, "yes", 2, 12, 1, 4),
      (0, "female", 32, 15, "yes", 5, 18, 6, 4),
      (0, "male", 27, 7, "yes", 5, 12, 5, 3),
      (0, "male", 52, 15, "yes", 2, 18, 5, 4),
      (0, "male", 27, 4, "no", 3, 20, 6, 3),
      (0, "male", 37, 4, "yes", 1, 18, 5, 4),
      (0, "male", 27, 4, "yes", 4, 14, 5, 4),
      (0, "female", 52, 15, "yes", 5, 12, 1, 3),
      (0, "female", 57, 15, "yes", 4, 16, 6, 4),
      (0, "male", 27, 7, "yes", 1, 16, 5, 4),
      (0, "male", 37, 7, "yes", 4, 20, 6, 3),
      (0, "male", 22, 0.75, "no", 2, 14, 4, 3),
      (0, "male", 32, 4, "yes", 2, 18, 5, 3),
      (0, "male", 37, 15, "yes", 4, 20, 6, 3),
      (0, "male", 22, 0.75, "yes", 2, 14, 4, 3),
      (0, "male", 42, 15, "yes", 4, 20, 6, 3),
      (0, "female", 52, 15, "yes", 5, 17, 1, 1),
      (0, "female", 37, 15, "yes", 4, 14, 1, 2),
      (0, "male", 27, 7, "yes", 4, 14, 5, 3),
      (0, "male", 32, 4, "yes", 2, 16, 5, 5),
      (0, "female", 27, 4, "yes", 2, 18, 6, 5),
      (0, "female", 27, 4, "yes", 2, 18, 5, 5),
      (0, "male", 37, 15, "yes", 5, 18, 6, 5),
      (0, "female", 47, 15, "yes", 5, 12, 5, 4),
      (0, "female", 32, 10, "yes", 3, 17, 1, 4),
      (0, "female", 27, 1.5, "yes", 4, 17, 1, 2),
      (0, "female", 57, 15, "yes", 2, 18, 5, 2),
      (0, "female", 22, 1.5, "no", 4, 14, 5, 4),
      (0, "male", 42, 15, "yes", 3, 14, 3, 4),
      (0, "male", 57, 15, "yes", 4, 9, 2, 2),
      (0, "male", 57, 15, "yes", 4, 20, 6, 5),
      (0, "female", 22, 0.125, "no", 4, 14, 4, 5),
      (0, "female", 32, 10, "yes", 4, 14, 1, 5),
      (0, "female", 42, 15, "yes", 3, 18, 5, 4),
      (0, "female", 27, 1.5, "no", 2, 18, 6, 5),
      (0, "male", 32, 0.125, "yes", 2, 18, 5, 2),
      (0, "female", 27, 4, "no", 3, 16, 5, 4),
      (0, "female", 27, 10, "yes", 2, 16, 1, 4),
      (0, "female", 32, 7, "yes", 4, 16, 1, 3),
      (0, "female", 37, 15, "yes", 4, 14, 5, 4),
      (0, "female", 42, 15, "yes", 5, 17, 6, 2),
      (0, "male", 32, 1.5, "yes", 4, 14, 6, 5),
      (0, "female", 32, 4, "yes", 3, 17, 5, 3),
      (0, "female", 37, 7, "no", 4, 18, 5, 5),
      (0, "female", 22, 0.417, "yes", 3, 14, 3, 5),
      (0, "female", 27, 7, "yes", 4, 14, 1, 5),
      (0, "male", 27, 0.75, "no", 3, 16, 5, 5),
      (0, "male", 27, 4, "yes", 2, 20, 5, 5),
      (0, "male", 32, 10, "yes", 4, 16, 4, 5),
      (0, "male", 32, 15, "yes", 1, 14, 5, 5),
      (0, "male", 22, 0.75, "no", 3, 17, 4, 5),
      (0, "female", 27, 7, "yes", 4, 17, 1, 4),
      (0, "male", 27, 0.417, "yes", 4, 20, 5, 4),
      (0, "male", 37, 15, "yes", 4, 20, 5, 4),
      (0, "female", 37, 15, "yes", 2, 14, 1, 3),
      (0, "male", 22, 4, "yes", 1, 18, 5, 4),
      (0, "male", 37, 15, "yes", 4, 17, 5, 3),
      (0, "female", 22, 1.5, "no", 2, 14, 4, 5),
      (0, "male", 52, 15, "yes", 4, 14, 6, 2),
      (0, "female", 22, 1.5, "no", 4, 17, 5, 5),
      (0, "male", 32, 4, "yes", 5, 14, 3, 5),
      (0, "male", 32, 4, "yes", 2, 14, 3, 5),
      (0, "female", 22, 1.5, "no", 3, 16, 6, 5),
      (0, "male", 27, 0.75, "no", 2, 18, 3, 3),
      (0, "female", 22, 7, "yes", 2, 14, 5, 2),
      (0, "female", 27, 0.75, "no", 2, 17, 5, 3),
      (0, "female", 37, 15, "yes", 4, 12, 1, 2),
      (0, "female", 22, 1.5, "no", 1, 14, 1, 5),
      (0, "female", 37, 10, "no", 2, 12, 4, 4),
      (0, "female", 37, 15, "yes", 4, 18, 5, 3),
      (0, "female", 42, 15, "yes", 3, 12, 3, 3),
      (0, "male", 22, 4, "no", 2, 18, 5, 5),
      (0, "male", 52, 7, "yes", 2, 20, 6, 2),
      (0, "male", 27, 0.75, "no", 2, 17, 5, 5),
      (0, "female", 27, 4, "no", 2, 17, 4, 5),
      (0, "male", 42, 1.5, "no", 5, 20, 6, 5),
      (0, "male", 22, 1.5, "no", 4, 17, 6, 5),
      (0, "male", 22, 4, "no", 4, 17, 5, 3),
      (0, "female", 22, 4, "yes", 1, 14, 5, 4),
      (0, "male", 37, 15, "yes", 5, 20, 4, 5),
      (0, "female", 37, 10, "yes", 3, 16, 6, 3),
      (0, "male", 42, 15, "yes", 4, 17, 6, 5),
      (0, "female", 47, 15, "yes", 4, 17, 5, 5),
      (0, "male", 22, 1.5, "no", 4, 16, 5, 4),
      (0, "female", 32, 10, "yes", 3, 12, 1, 4),
      (0, "female", 22, 7, "yes", 1, 14, 3, 5),
      (0, "female", 32, 10, "yes", 4, 17, 5, 4),
      (0, "male", 27, 1.5, "yes", 2, 16, 2, 4),
      (0, "male", 37, 15, "yes", 4, 14, 5, 5),
      (0, "male", 42, 4, "yes", 3, 14, 4, 5),
      (0, "female", 37, 15, "yes", 5, 14, 5, 4),
      (0, "female", 32, 7, "yes", 4, 17, 5, 5),
      (0, "female", 42, 15, "yes", 4, 18, 6, 5),
      (0, "male", 27, 4, "no", 4, 18, 6, 4),
      (0, "male", 22, 0.75, "no", 4, 18, 6, 5),
      (0, "male", 27, 4, "yes", 4, 14, 5, 3),
      (0, "female", 22, 0.75, "no", 5, 18, 1, 5),
      (0, "female", 52, 15, "yes", 5, 9, 5, 5),
      (0, "male", 32, 10, "yes", 3, 14, 5, 5),
      (0, "female", 37, 15, "yes", 4, 16, 4, 4),
      (0, "male", 32, 7, "yes", 2, 20, 5, 4),
      (0, "female", 42, 15, "yes", 3, 18, 1, 4),
      (0, "male", 32, 15, "yes", 1, 16, 5, 5),
      (0, "male", 27, 4, "yes", 3, 18, 5, 5),
      (0, "female", 32, 15, "yes", 4, 12, 3, 4),
      (0, "male", 22, 0.75, "yes", 3, 14, 2, 4),
      (0, "female", 22, 1.5, "no", 3, 16, 5, 3),
      (0, "female", 42, 15, "yes", 4, 14, 3, 5),
      (0, "female", 52, 15, "yes", 3, 16, 5, 4),
      (0, "male", 37, 15, "yes", 5, 20, 6, 4),
      (0, "female", 47, 15, "yes", 4, 12, 2, 3),
      (0, "male", 57, 15, "yes", 2, 20, 6, 4),
      (0, "male", 32, 7, "yes", 4, 17, 5, 5),
      (0, "female", 27, 7, "yes", 4, 17, 1, 4),
      (0, "male", 22, 1.5, "no", 1, 18, 6, 5),
      (0, "female", 22, 4, "yes", 3, 9, 1, 4),
      (0, "female", 22, 1.5, "no", 2, 14, 1, 5),
      (0, "male", 42, 15, "yes", 2, 20, 6, 4),
      (0, "male", 57, 15, "yes", 4, 9, 2, 4),
      (0, "female", 27, 7, "yes", 2, 18, 1, 5),
      (0, "female", 22, 4, "yes", 3, 14, 1, 5),
      (0, "male", 37, 15, "yes", 4, 14, 5, 3),
      (0, "male", 32, 7, "yes", 1, 18, 6, 4),
      (0, "female", 22, 1.5, "no", 2, 14, 5, 5),
      (0, "female", 22, 1.5, "yes", 3, 12, 1, 3),
      (0, "male", 52, 15, "yes", 2, 14, 5, 5),
      (0, "female", 37, 15, "yes", 2, 14, 1, 1),
      (0, "female", 32, 10, "yes", 2, 14, 5, 5),
      (0, "male", 42, 15, "yes", 4, 20, 4, 5),
      (0, "female", 27, 4, "yes", 3, 18, 4, 5),
      (0, "male", 37, 15, "yes", 4, 20, 6, 5),
      (0, "male", 27, 1.5, "no", 3, 18, 5, 5),
      (0, "female", 22, 0.125, "no", 2, 16, 6, 3),
      (0, "male", 32, 10, "yes", 2, 20, 6, 3),
      (0, "female", 27, 4, "no", 4, 18, 5, 4),
      (0, "female", 27, 7, "yes", 2, 12, 5, 1),
      (0, "male", 32, 4, "yes", 5, 18, 6, 3),
      (0, "female", 37, 15, "yes", 2, 17, 5, 5),
      (0, "male", 47, 15, "no", 4, 20, 6, 4),
      (0, "male", 27, 1.5, "no", 1, 18, 5, 5),
      (0, "male", 37, 15, "yes", 4, 20, 6, 4),
      (0, "female", 32, 15, "yes", 4, 18, 1, 4),
      (0, "female", 32, 7, "yes", 4, 17, 5, 4),
      (0, "female", 42, 15, "yes", 3, 14, 1, 3),
      (0, "female", 27, 7, "yes", 3, 16, 1, 4),
      (0, "male", 27, 1.5, "no", 3, 16, 4, 2),
      (0, "male", 22, 1.5, "no", 3, 16, 3, 5),
      (0, "male", 27, 4, "yes", 3, 16, 4, 2),
      (0, "female", 27, 7, "yes", 3, 12, 1, 2),
      (0, "female", 37, 15, "yes", 2, 18, 5, 4),
      (0, "female", 37, 7, "yes", 3, 14, 4, 4),
      (0, "male", 22, 1.5, "no", 2, 16, 5, 5),
      (0, "male", 37, 15, "yes", 5, 20, 5, 4),
      (0, "female", 22, 1.5, "no", 4, 16, 5, 3),
      (0, "female", 32, 10, "yes", 4, 16, 1, 5),
      (0, "male", 27, 4, "no", 2, 17, 5, 3),
      (0, "female", 22, 0.417, "no", 4, 14, 5, 5),
      (0, "female", 27, 4, "no", 2, 18, 5, 5),
      (0, "male", 37, 15, "yes", 4, 18, 5, 3),
      (0, "male", 37, 10, "yes", 5, 20, 7, 4),
      (0, "female", 27, 7, "yes", 2, 14, 4, 2),
      (0, "male", 32, 4, "yes", 2, 16, 5, 5),
      (0, "male", 32, 4, "yes", 2, 16, 6, 4),
      (0, "male", 22, 1.5, "no", 3, 18, 4, 5),
      (0, "female", 22, 4, "yes", 4, 14, 3, 4),
      (0, "female", 17.5, 0.75, "no", 2, 18, 5, 4),
      (0, "male", 32, 10, "yes", 4, 20, 4, 5),
      (0, "female", 32, 0.75, "no", 5, 14, 3, 3),
      (0, "male", 37, 15, "yes", 4, 17, 5, 3),
      (0, "male", 32, 4, "no", 3, 14, 4, 5),
      (0, "female", 27, 1.5, "no", 2, 17, 3, 2),
      (0, "female", 22, 7, "yes", 4, 14, 1, 5),
      (0, "male", 47, 15, "yes", 5, 14, 6, 5),
      (0, "male", 27, 4, "yes", 1, 16, 4, 4),
      (0, "female", 37, 15, "yes", 5, 14, 1, 3),
      (0, "male", 42, 4, "yes", 4, 18, 5, 5),
      (0, "female", 32, 4, "yes", 2, 14, 1, 5),
      (0, "male", 52, 15, "yes", 2, 14, 7, 4),
      (0, "female", 22, 1.5, "no", 2, 16, 1, 4),
      (0, "male", 52, 15, "yes", 4, 12, 2, 4),
      (0, "female", 22, 0.417, "no", 3, 17, 1, 5),
      (0, "female", 22, 1.5, "no", 2, 16, 5, 5),
      (0, "male", 27, 4, "yes", 4, 20, 6, 4),
      (0, "female", 32, 15, "yes", 4, 14, 1, 5),
      (0, "female", 27, 1.5, "no", 2, 16, 3, 5),
      (0, "male", 32, 4, "no", 1, 20, 6, 5),
      (0, "male", 37, 15, "yes", 3, 20, 6, 4),
      (0, "female", 32, 10, "no", 2, 16, 6, 5),
      (0, "female", 32, 10, "yes", 5, 14, 5, 5),
      (0, "male", 37, 1.5, "yes", 4, 18, 5, 3),
      (0, "male", 32, 1.5, "no", 2, 18, 4, 4),
      (0, "female", 32, 10, "yes", 4, 14, 1, 4),
      (0, "female", 47, 15, "yes", 4, 18, 5, 4),
      (0, "female", 27, 10, "yes", 5, 12, 1, 5),
      (0, "male", 27, 4, "yes", 3, 16, 4, 5),
      (0, "female", 37, 15, "yes", 4, 12, 4, 2),
      (0, "female", 27, 0.75, "no", 4, 16, 5, 5),
      (0, "female", 37, 15, "yes", 4, 16, 1, 5),
      (0, "female", 32, 15, "yes", 3, 16, 1, 5),
      (0, "female", 27, 10, "yes", 2, 16, 1, 5),
      (0, "male", 27, 7, "no", 2, 20, 6, 5),
      (0, "female", 37, 15, "yes", 2, 14, 1, 3),
      (0, "male", 27, 1.5, "yes", 2, 17, 4, 4),
      (0, "female", 22, 0.75, "yes", 2, 14, 1, 5),
      (0, "male", 22, 4, "yes", 4, 14, 2, 4),
      (0, "male", 42, 0.125, "no", 4, 17, 6, 4),
      (0, "male", 27, 1.5, "yes", 4, 18, 6, 5),
      (0, "male", 27, 7, "yes", 3, 16, 6, 3),
      (0, "female", 52, 15, "yes", 4, 14, 1, 3),
      (0, "male", 27, 1.5, "no", 5, 20, 5, 2),
      (0, "female", 27, 1.5, "no", 2, 16, 5, 5),
      (0, "female", 27, 1.5, "no", 3, 17, 5, 5),
      (0, "male", 22, 0.125, "no", 5, 16, 4, 4),
      (0, "female", 27, 4, "yes", 4, 16, 1, 5),
      (0, "female", 27, 4, "yes", 4, 12, 1, 5),
      (0, "female", 47, 15, "yes", 2, 14, 5, 5),
      (0, "female", 32, 15, "yes", 3, 14, 5, 3),
      (0, "male", 42, 7, "yes", 2, 16, 5, 5),
      (0, "male", 22, 0.75, "no", 4, 16, 6, 4),
      (0, "male", 27, 0.125, "no", 3, 20, 6, 5),
      (0, "male", 32, 10, "yes", 3, 20, 6, 5),
      (0, "female", 22, 0.417, "no", 5, 14, 4, 5),
      (0, "female", 47, 15, "yes", 5, 14, 1, 4),
      (0, "female", 32, 10, "yes", 3, 14, 1, 5),
      (0, "male", 57, 15, "yes", 4, 17, 5, 5),
      (0, "male", 27, 4, "yes", 3, 20, 6, 5),
      (0, "female", 32, 7, "yes", 4, 17, 1, 5),
      (0, "female", 37, 10, "yes", 4, 16, 1, 5),
      (0, "female", 32, 10, "yes", 1, 18, 1, 4),
      (0, "female", 22, 4, "no", 3, 14, 1, 4),
      (0, "female", 27, 7, "yes", 4, 14, 3, 2),
      (0, "male", 57, 15, "yes", 5, 18, 5, 2),
      (0, "male", 32, 7, "yes", 2, 18, 5, 5),
      (0, "female", 27, 1.5, "no", 4, 17, 1, 3),
      (0, "male", 22, 1.5, "no", 4, 14, 5, 5),
      (0, "female", 22, 1.5, "yes", 4, 14, 5, 4),
      (0, "female", 32, 7, "yes", 3, 16, 1, 5),
      (0, "female", 47, 15, "yes", 3, 16, 5, 4),
      (0, "female", 22, 0.75, "no", 3, 16, 1, 5),
      (0, "female", 22, 1.5, "yes", 2, 14, 5, 5),
      (0, "female", 27, 4, "yes", 1, 16, 5, 5),
      (0, "male", 52, 15, "yes", 4, 16, 5, 5),
      (0, "male", 32, 10, "yes", 4, 20, 6, 5),
      (0, "male", 47, 15, "yes", 4, 16, 6, 4),
      (0, "female", 27, 7, "yes", 2, 14, 1, 2),
      (0, "female", 22, 1.5, "no", 4, 14, 4, 5),
      (0, "female", 32, 10, "yes", 2, 16, 5, 4),
      (0, "female", 22, 0.75, "no", 2, 16, 5, 4),
      (0, "female", 22, 1.5, "no", 2, 16, 5, 5),
      (0, "female", 42, 15, "yes", 3, 18, 6, 4),
      (0, "female", 27, 7, "yes", 5, 14, 4, 5),
      (0, "male", 42, 15, "yes", 4, 16, 4, 4),
      (0, "female", 57, 15, "yes", 3, 18, 5, 2),
      (0, "male", 42, 15, "yes", 3, 18, 6, 2),
      (0, "female", 32, 7, "yes", 2, 14, 1, 2),
      (0, "male", 22, 4, "no", 5, 12, 4, 5),
      (0, "female", 22, 1.5, "no", 1, 16, 6, 5),
      (0, "female", 22, 0.75, "no", 1, 14, 4, 5),
      (0, "female", 32, 15, "yes", 4, 12, 1, 5),
      (0, "male", 22, 1.5, "no", 2, 18, 5, 3),
      (0, "male", 27, 4, "yes", 5, 17, 2, 5),
      (0, "female", 27, 4, "yes", 4, 12, 1, 5),
      (0, "male", 42, 15, "yes", 5, 18, 5, 4),
      (0, "male", 32, 1.5, "no", 2, 20, 7, 3),
      (0, "male", 57, 15, "no", 4, 9, 3, 1),
      (0, "male", 37, 7, "no", 4, 18, 5, 5),
      (0, "male", 52, 15, "yes", 2, 17, 5, 4),
      (0, "male", 47, 15, "yes", 4, 17, 6, 5),
      (0, "female", 27, 7, "no", 2, 17, 5, 4),
      (0, "female", 27, 7, "yes", 4, 14, 5, 5),
      (0, "female", 22, 4, "no", 2, 14, 3, 3),
      (0, "male", 37, 7, "yes", 2, 20, 6, 5),
      (0, "male", 27, 7, "no", 4, 12, 4, 3),
      (0, "male", 42, 10, "yes", 4, 18, 6, 4),
      (0, "female", 22, 1.5, "no", 3, 14, 1, 5),
      (0, "female", 22, 4, "yes", 2, 14, 1, 3),
      (0, "female", 57, 15, "no", 4, 20, 6, 5),
      (0, "male", 37, 15, "yes", 4, 14, 4, 3),
      (0, "female", 27, 7, "yes", 3, 18, 5, 5),
      (0, "female", 17.5, 10, "no", 4, 14, 4, 5),
      (0, "male", 22, 4, "yes", 4, 16, 5, 5),
      (0, "female", 27, 4, "yes", 2, 16, 1, 4),
      (0, "female", 37, 15, "yes", 2, 14, 5, 1),
      (0, "female", 22, 1.5, "no", 5, 14, 1, 4),
      (0, "male", 27, 7, "yes", 2, 20, 5, 4),
      (0, "male", 27, 4, "yes", 4, 14, 5, 5),
      (0, "male", 22, 0.125, "no", 1, 16, 3, 5),
      (0, "female", 27, 7, "yes", 4, 14, 1, 4),
      (0, "female", 32, 15, "yes", 5, 16, 5, 3),
      (0, "male", 32, 10, "yes", 4, 18, 5, 4),
      (0, "female", 32, 15, "yes", 2, 14, 3, 4),
      (0, "female", 22, 1.5, "no", 3, 17, 5, 5),
      (0, "male", 27, 4, "yes", 4, 17, 4, 4),
      (0, "female", 52, 15, "yes", 5, 14, 1, 5),
      (0, "female", 27, 7, "yes", 2, 12, 1, 2),
      (0, "female", 27, 7, "yes", 3, 12, 1, 4),
      (0, "female", 42, 15, "yes", 2, 14, 1, 4),
      (0, "female", 42, 15, "yes", 4, 14, 5, 4),
      (0, "male", 27, 7, "yes", 4, 14, 3, 3),
      (0, "male", 27, 7, "yes", 2, 20, 6, 2),
      (0, "female", 42, 15, "yes", 3, 12, 3, 3),
      (0, "male", 27, 4, "yes", 3, 16, 3, 5),
      (0, "female", 27, 7, "yes", 3, 14, 1, 4),
      (0, "female", 22, 1.5, "no", 2, 14, 4, 5),
      (0, "female", 27, 4, "yes", 4, 14, 1, 4),
      (0, "female", 22, 4, "no", 4, 14, 5, 5),
      (0, "female", 22, 1.5, "no", 2, 16, 4, 5),
      (0, "male", 47, 15, "no", 4, 14, 5, 4),
      (0, "male", 37, 10, "yes", 2, 18, 6, 2),
      (0, "male", 37, 15, "yes", 3, 17, 5, 4),
      (0, "female", 27, 4, "yes", 2, 16, 1, 4),
      (3, "male", 27, 1.5, "no", 3, 18, 4, 4),
      (3, "female", 27, 4, "yes", 3, 17, 1, 5),
      (7, "male", 37, 15, "yes", 5, 18, 6, 2),
      (12, "female", 32, 10, "yes", 3, 17, 5, 2),
      (1, "male", 22, 0.125, "no", 4, 16, 5, 5),
      (1, "female", 22, 1.5, "yes", 2, 14, 1, 5),
      (12, "male", 37, 15, "yes", 4, 14, 5, 2),
      (7, "female", 22, 1.5, "no", 2, 14, 3, 4),
      (2, "male", 37, 15, "yes", 2, 18, 6, 4),
      (3, "female", 32, 15, "yes", 4, 12, 3, 2),
      (1, "female", 37, 15, "yes", 4, 14, 4, 2),
      (7, "female", 42, 15, "yes", 3, 17, 1, 4),
      (12, "female", 42, 15, "yes", 5, 9, 4, 1),
      (12, "male", 37, 10, "yes", 2, 20, 6, 2),
      (12, "female", 32, 15, "yes", 3, 14, 1, 2),
      (3, "male", 27, 4, "no", 1, 18, 6, 5),
      (7, "male", 37, 10, "yes", 2, 18, 7, 3),
      (7, "female", 27, 4, "no", 3, 17, 5, 5),
      (1, "male", 42, 15, "yes", 4, 16, 5, 5),
      (1, "female", 47, 15, "yes", 5, 14, 4, 5),
      (7, "female", 27, 4, "yes", 3, 18, 5, 4),
      (1, "female", 27, 7, "yes", 5, 14, 1, 4),
      (12, "male", 27, 1.5, "yes", 3, 17, 5, 4),
      (12, "female", 27, 7, "yes", 4, 14, 6, 2),
      (3, "female", 42, 15, "yes", 4, 16, 5, 4),
      (7, "female", 27, 10, "yes", 4, 12, 7, 3),
      (1, "male", 27, 1.5, "no", 2, 18, 5, 2),
      (1, "male", 32, 4, "no", 4, 20, 6, 4),
      (1, "female", 27, 7, "yes", 3, 14, 1, 3),
      (3, "female", 32, 10, "yes", 4, 14, 1, 4),
      (3, "male", 27, 4, "yes", 2, 18, 7, 2),
      (1, "female", 17.5, 0.75, "no", 5, 14, 4, 5),
      (1, "female", 32, 10, "yes", 4, 18, 1, 5),
      (7, "female", 32, 7, "yes", 2, 17, 6, 4),
      (7, "male", 37, 15, "yes", 2, 20, 6, 4),
      (7, "female", 37, 10, "no", 1, 20, 5, 3),
      (12, "female", 32, 10, "yes", 2, 16, 5, 5),
      (7, "male", 52, 15, "yes", 2, 20, 6, 4),
      (7, "female", 42, 15, "yes", 1, 12, 1, 3),
      (1, "male", 52, 15, "yes", 2, 20, 6, 3),
      (2, "male", 37, 15, "yes", 3, 18, 6, 5),
      (12, "female", 22, 4, "no", 3, 12, 3, 4),
      (12, "male", 27, 7, "yes", 1, 18, 6, 2),
      (1, "male", 27, 4, "yes", 3, 18, 5, 5),
      (12, "male", 47, 15, "yes", 4, 17, 6, 5),
      (12, "female", 42, 15, "yes", 4, 12, 1, 1),
      (7, "male", 27, 4, "no", 3, 14, 3, 4),
      (7, "female", 32, 7, "yes", 4, 18, 4, 5),
      (1, "male", 32, 0.417, "yes", 3, 12, 3, 4),
      (3, "male", 47, 15, "yes", 5, 16, 5, 4),
      (12, "male", 37, 15, "yes", 2, 20, 5, 4),
      (7, "male", 22, 4, "yes", 2, 17, 6, 4),
      (1, "male", 27, 4, "no", 2, 14, 4, 5),
      (7, "female", 52, 15, "yes", 5, 16, 1, 3),
      (1, "male", 27, 4, "no", 3, 14, 3, 3),
      (1, "female", 27, 10, "yes", 4, 16, 1, 4),
      (1, "male", 32, 7, "yes", 3, 14, 7, 4),
      (7, "male", 32, 7, "yes", 2, 18, 4, 1),
      (3, "male", 22, 1.5, "no", 1, 14, 3, 2),
      (7, "male", 22, 4, "yes", 3, 18, 6, 4),
      (7, "male", 42, 15, "yes", 4, 20, 6, 4),
      (2, "female", 57, 15, "yes", 1, 18, 5, 4),
      (7, "female", 32, 4, "yes", 3, 18, 5, 2),
      (1, "male", 27, 4, "yes", 1, 16, 4, 4),
      (7, "male", 32, 7, "yes", 4, 16, 1, 4),
      (2, "male", 57, 15, "yes", 1, 17, 4, 4),
      (7, "female", 42, 15, "yes", 4, 14, 5, 2),
      (7, "male", 37, 10, "yes", 1, 18, 5, 3),
      (3, "male", 42, 15, "yes", 3, 17, 6, 1),
      (1, "female", 52, 15, "yes", 3, 14, 4, 4),
      (2, "female", 27, 7, "yes", 3, 17, 5, 3),
      (12, "male", 32, 7, "yes", 2, 12, 4, 2),
      (1, "male", 22, 4, "no", 4, 14, 2, 5),
      (3, "male", 27, 7, "yes", 3, 18, 6, 4),
      (12, "female", 37, 15, "yes", 1, 18, 5, 5),
      (7, "female", 32, 15, "yes", 3, 17, 1, 3),
      (7, "female", 27, 7, "no", 2, 17, 5, 5),
      (1, "female", 32, 7, "yes", 3, 17, 5, 3),
      (1, "male", 32, 1.5, "yes", 2, 14, 2, 4),
      (12, "female", 42, 15, "yes", 4, 14, 1, 2),
      (7, "male", 32, 10, "yes", 3, 14, 5, 4),
      (7, "male", 37, 4, "yes", 1, 20, 6, 3),
      (1, "female", 27, 4, "yes", 2, 16, 5, 3),
      (12, "female", 42, 15, "yes", 3, 14, 4, 3),
      (1, "male", 27, 10, "yes", 5, 20, 6, 5),
      (12, "male", 37, 10, "yes", 2, 20, 6, 2),
      (12, "female", 27, 7, "yes", 1, 14, 3, 3),
      (3, "female", 27, 7, "yes", 4, 12, 1, 2),
      (3, "male", 32, 10, "yes", 2, 14, 4, 4),
      (12, "female", 17.5, 0.75, "yes", 2, 12, 1, 3),
      (12, "female", 32, 15, "yes", 3, 18, 5, 4),
      (2, "female", 22, 7, "no", 4, 14, 4, 3),
      (1, "male", 32, 7, "yes", 4, 20, 6, 5),
      (7, "male", 27, 4, "yes", 2, 18, 6, 2),
      (1, "female", 22, 1.5, "yes", 5, 14, 5, 3),
      (12, "female", 32, 15, "no", 3, 17, 5, 1),
      (12, "female", 42, 15, "yes", 2, 12, 1, 2),
      (7, "male", 42, 15, "yes", 3, 20, 5, 4),
      (12, "male", 32, 10, "no", 2, 18, 4, 2),
      (12, "female", 32, 15, "yes", 3, 9, 1, 1),
      (7, "male", 57, 15, "yes", 5, 20, 4, 5),
      (12, "male", 47, 15, "yes", 4, 20, 6, 4),
      (2, "female", 42, 15, "yes", 2, 17, 6, 3),
      (12, "male", 37, 15, "yes", 3, 17, 6, 3),
      (12, "male", 37, 15, "yes", 5, 17, 5, 2),
      (7, "male", 27, 10, "yes", 2, 20, 6, 4),
      (2, "male", 37, 15, "yes", 2, 16, 5, 4),
      (12, "female", 32, 15, "yes", 1, 14, 5, 2),
      (7, "male", 32, 10, "yes", 3, 17, 6, 3),
      (2, "male", 37, 15, "yes", 4, 18, 5, 1),
      (7, "female", 27, 1.5, "no", 2, 17, 5, 5),
      (3, "female", 47, 15, "yes", 2, 17, 5, 2),
      (12, "male", 37, 15, "yes", 2, 17, 5, 4),
      (12, "female", 27, 4, "no", 2, 14, 5, 5),
      (2, "female", 27, 10, "yes", 4, 14, 1, 5),
      (1, "female", 22, 4, "yes", 3, 16, 1, 3),
      (12, "male", 52, 7, "no", 4, 16, 5, 5),
      (2, "female", 27, 4, "yes", 1, 16, 3, 5),
      (7, "female", 37, 15, "yes", 2, 17, 6, 4),
      (2, "female", 27, 4, "no", 1, 17, 3, 1),
      (12, "female", 17.5, 0.75, "yes", 2, 12, 3, 5),
      (7, "female", 32, 15, "yes", 5, 18, 5, 4),
      (7, "female", 22, 4, "no", 1, 16, 3, 5),
      (2, "male", 32, 4, "yes", 4, 18, 6, 4),
      (1, "female", 22, 1.5, "yes", 3, 18, 5, 2),
      (3, "female", 42, 15, "yes", 2, 17, 5, 4),
      (1, "male", 32, 7, "yes", 4, 16, 4, 4),
      (12, "male", 37, 15, "no", 3, 14, 6, 2),
      (1, "male", 42, 15, "yes", 3, 16, 6, 3),
      (1, "male", 27, 4, "yes", 1, 18, 5, 4),
      (2, "male", 37, 15, "yes", 4, 20, 7, 3),
      (7, "male", 37, 15, "yes", 3, 20, 6, 4),
      (3, "male", 22, 1.5, "no", 2, 12, 3, 3),
      (3, "male", 32, 4, "yes", 3, 20, 6, 2),
      (2, "male", 32, 15, "yes", 5, 20, 6, 5),
      (12, "female", 52, 15, "yes", 1, 18, 5, 5),
      (12, "male", 47, 15, "no", 1, 18, 6, 5),
      (3, "female", 32, 15, "yes", 4, 16, 4, 4),
      (7, "female", 32, 15, "yes", 3, 14, 3, 2),
      (7, "female", 27, 7, "yes", 4, 16, 1, 2),
      (12, "male", 42, 15, "yes", 3, 18, 6, 2),
      (7, "female", 42, 15, "yes", 2, 14, 3, 2),
      (12, "male", 27, 7, "yes", 2, 17, 5, 4),
      (3, "male", 32, 10, "yes", 4, 14, 4, 3),
      (7, "male", 47, 15, "yes", 3, 16, 4, 2),
      (1, "male", 22, 1.5, "yes", 1, 12, 2, 5),
      (7, "female", 32, 10, "yes", 2, 18, 5, 4),
      (2, "male", 32, 10, "yes", 2, 17, 6, 5),
      (2, "male", 22, 7, "yes", 3, 18, 6, 2),
      (1, "female", 32, 15, "yes", 3, 14, 1, 5)
    )

    import spark.implicits._
    val data = dataList.toDF("affairs", "gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")
    data.createOrReplaceTempView("data")

//    data.printSchema()
//
//    data.show(10, false)
//
//    val descDF = data.describe("affairs", "gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")
//    descDF.selectExpr("summary",
//      "round(affairs,2) as affairs",
//      "round(age,2) as age",
//      "round(yearsmarried,2) as yearsmarried",
//      "children",
//      "round(religiousness,2) as religiousness",
//      "round(education,2) as education",
//      "round(occupation,2) as occupation",
//      "round(rating,2) as rating")
//      .show(10, truncate = false)

//    字符类型转换成数值
    val labelWhere = "case when affairs=0 then 0 else cast(1 as double) end as label"
    val genderWhere = "case when gender='female' then 0 else cast(1 as double) end as gender"
    val childrenWhere = "case when children='no' then 0 else cast(1 as double) end as children"

    val dataLabelDF = spark.sql(s"select $labelWhere, $genderWhere,age,yearsmarried,$childrenWhere,religiousness,education,occupation,rating from data")


//创建决策树模型

    val featuresArray = Array("gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")

//    字段转换成特征向量
    val assembler = new VectorAssembler()
      .setInputCols(featuresArray)
      .setOutputCol("features")
    val vecDF = assembler.transform(dataLabelDF)
    vecDF.show(10, false)

//    索引标签，将元数据加到标签列中
    val lableIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel").fit(vecDF)

//    自动识别分类的特征，并对他们进行索引
//    具有大于5个不同的值的特征被视为连续
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(5)
      .fit(vecDF)

//    将数据按比例分为训练集和测试集
    val Array(trainingData, testData) = vecDF.randomSplit(Array(0.7, 0.3))

//    训练决策树模型
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setImpurity("entropy")
      .setMaxBins(100)
      .setMaxDepth(5)
      .setMinInfoGain(0.01)
      .setMinInstancesPerNode(10)
      .setSeed(123456L)

//    将索引标签转换成原始标签
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(lableIndexer.labels)

//    创建管道流
    val pipeline = new Pipeline().setStages(Array(lableIndexer,featureIndexer, dt, labelConverter))

//    对训练集进行训练模型
    val model = pipeline.fit(trainingData)

//    对测试集进行预测
    val predictions = model.transform(testData)

//    选择几个测试示例进行展示
    predictions.select("predictedLabel", "label", "features").show(10, false)

//    选择(预测标签, 实际标签)，并计算测试误差
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

  }

}

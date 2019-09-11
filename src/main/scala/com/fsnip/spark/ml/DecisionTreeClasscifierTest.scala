package com.fsnip.spark.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 11:34 2019/1/25
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object DecisionTreeClasscifierTest {

  def main(args: Array[String]): Unit = {

    // 设置打印日志的级别，屏蔽掉不需要打印的日志
    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)

    val spark = SparkSession.builder().appName("DesionTreeTest").master("local[*]").getOrCreate()

    val data = spark.read.format("libsvm").load("file:///F:\\spark-2.3.0-bin-hadoop2.7\\data\\mllib\\sample_libsvm_data.txt")

    // 运用字符串索引对特征值进行转换
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)

    // 运用向量索引对特征值进行转换
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)

    // 随机切分为训练数据和测试数据
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // 训练决策树模型
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // 创建流水线
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

    val model = pipeline.fit(trainingData)

    val predictions = model.transform(testData)

    predictions.select("predictedLabel", "label", "features").show(5, false)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${(1.0 - accuracy)}")

    val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println(s"Learn classification tree model:\n ${treeModel.toDebugString}")

    Thread.sleep(100000)

    spark.stop()
  }

}

package com.fsnip.spark.ml

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vectors, Vector}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 17:15 2019/1/22
  * @ Description：Estimator, Transformer, and Param测试
  * @ Modified By：
  * @ Version:     
  */
object EstimatorTransformerParamExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("EstimatorTransformerParamExample")
      .master("local[*]")
      .getOrCreate()

//    import spark.implicits._

    val training = spark.createDataFrame(

      Seq(
        (1.0, Vectors.dense(0.0, 1.1, 0.1)),
        (0.0, Vectors.dense(2.0, 1.0, -1.0)),
        (0.0, Vectors.dense(2.0, 1.3, 1.0)),
        (1.0, Vectors.dense(0.0, 1.2, -0.5))
      )
    ).toDF("label", "features")

//    创建一个逻辑回归实例-估计器
    val lr = new LogisticRegression()
    println("LogisticRegression:\n" + lr.explainParams() + "\n")

//    通过方法设置参数
    lr.setMaxIter(10).setRegParam(0.01)

//    训练一个回归模型，保存在lr中
    val model1 = lr.fit(training)
    println("Model 1 was fit using parameters: " + model1.parent.extractParamMap)

    // We may alternatively specify parameters using a ParamMap,
    // which supports several methods for specifying parameters.
    val paramMap = ParamMap(lr.maxIter -> 20)
      .put(lr.maxIter, 30)  // Specify 1 Param. This overwrites the original maxIter.
      .put(lr.regParam -> 0.1, lr.threshold -> 0.55)  // Specify multiple Params.

    //合并ParamMaps
    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")
    val paramMapCombined = paramMap ++ paramMap2

    val model2 = lr.fit(training,paramMapCombined)
    println("Model 2 was fit using parameters: " + model2.parent.extractParamMap)

    val test = spark.createDataFrame(Seq(

      (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
      (0.0, Vectors.dense(3.0, 2.0, -0.1)),
      (1.0, Vectors.dense(0.0, 2.2, -1.5))

    )).toDF("label", "features")

//    model2.transform(test)
//        .select("features", "label", "myProbability", "prediction")
//        .collect()
//        .foreach(case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
//        println(s"($features, $label) -> prob=$prob, prediction=$prediction")
//    )

    model2.transform(test)
      .select("features", "label", "myProbability", "prediction")
      .collect()
      .foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
        println(s"($features, $label) -> prob=$prob, prediction=$prediction")
      }
  }

}

package com.fsnip.spark.ml.myml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 10:42 2019/7/19
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object LinearRegressionOnline {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("LinearRegressionOnline")
      .master("local[2]")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val sc = spark.sparkContext

    val raw_data = sc.textFile("file:///F:\\scala-workspace\\spark\\data\\LR\\LR_data")
    val map_data = raw_data.map{x =>
      val split_list = x.split(",")
      (split_list(0).toDouble,split_list(1).toDouble,split_list(2).toDouble,
        split_list(3).toDouble,split_list(4).toDouble,split_list(5).toDouble,
        split_list(6).toDouble,split_list(7).toDouble)
    }
//    val df  = spark.createDataFrame(map_data)
    import spark.implicits._
    val data =  map_data.toDF("Population", "Income", "Illiteracy", "LifeExp", "Murder", "HSGrad", "Frost", "Area")
    val colArray = Array("Population", "Income", "Illiteracy", "LifeExp", "HSGrad", "Frost", "Area")

    val assembler = new VectorAssembler()
      .setInputCols(colArray)
      .setOutputCol("features")
    val vecDF = assembler.transform(data)

    val lr = new LinearRegression()
      .setLabelCol("Murder")
      .setFeaturesCol("features")
      .setFitIntercept(true)
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    val lr_model = lr.fit(vecDF)

    lr_model.extractParamMap()
    println(s"Coefficients: ${lr_model.coefficients} Intercept: ${lr_model.intercept}")

    // 模型进行评价
    val trainingSummary = lr_model.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

//    准备预测集合
    val raw_data_predict = sc.textFile("file:///F:\\scala-workspace\\spark\\data\\LR\\LR_data_for_predic")
    val map_data_predict = raw_data_predict.map(x => {
      val split_list = x.split(",")
      (split_list(0).toDouble,split_list(1).toDouble,split_list(2).toDouble,
        split_list(3).toDouble,split_list(4).toDouble,split_list(5).toDouble,
        split_list(6).toDouble,split_list(7).toDouble)
    })
    import spark.implicits._
    val df_predict = map_data_predict.toDF("Population", "Income", "Illiteracy", "LifeExp", "Murder", "HSGrad", "Frost", "Area")
    val df_predict_colArray = Array("Population", "Income", "Illiteracy", "LifeExp", "HSGrad", "Frost", "Area")

    val assembler_predict = new VectorAssembler()
      .setInputCols(df_predict_colArray)
      .setOutputCol("features")
    val predict_vecDF = assembler_predict.transform(df_predict)

    val predictions = lr_model.transform(predict_vecDF)

    println("输出预测结果")
    val predict_result =predictions.selectExpr("features","Murder", "round(prediction,1) as prediction")
    predict_result.foreach(println(_))
    sc.stop()

  }

}

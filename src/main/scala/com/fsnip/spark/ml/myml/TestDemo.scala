package com.fsnip.spark.ml.myml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 17:39 2019/7/18
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object TestDemo {

  def labeledFile(originFile: String, sc: SparkContext): RDD[LabeledPoint] = {
    val file_load = sc.textFile(originFile)
    val file_split = file_load.map((_.split(",")))

//    构建映射类的函数方法
    def mapping(rdd: RDD[Array[String]], index: Int) = {
      rdd.map(x => x(index)).distinct().zipWithIndex().collect().toMap
    }
//    定义存储每列映射方法mapping的maps集合
    var maps: Map[Int, Map[String, Long]] = Map()
//    生成maps
    for(i <- 2 until 10)
      maps += (i -> mapping(file_split,i))
//    计算每列的特征值之和max_size
    val max_size = maps.map(x => x._2.size).sum
    val file_label = file_split.map{
      x =>{
        var num: Int = 0
        var size: Int = 0
//        构建长度为max_size+4的特征数组，初始值全为0
        val arrayOfDim = Array.ofDim[Double](max_size+4)
        for(j <- 2 until(10)){
          num = maps(j)(x(j)).toInt
          if(j==2) size=0 else size += maps(j-1).size
          /*为特征赋值*/
          arrayOfDim(size+num)=1.0
        }
        /*添加后面4列归一化的特征*/
        for(j<-10 until 14)
          arrayOfDim(max_size+(j-10))=x(j).toDouble
        /*生成LabeledPoint类型*/
        LabeledPoint(x(14).toDouble+x(15).toDouble,Vectors.dense(arrayOfDim))
      }
    }
    file_label
  }

  def main(args: Array[String]): Unit = {
    val spark= SparkSession
      .builder()
      .appName("TextClassify")
      .master("local[*]")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val sc = spark.sparkContext

    val file_bike = "hour_nohead.csv"

//    调用二元向量方法
    val labeled_file = labeledFile(file_bike, sc)

    val labeled_file1 = labeled_file.map(point => LabeledPoint(math.log(point.label), point.features))

//    val model_linear = LinearRegressionWithSGD.train(labeled_file, 10, 0.1)
  }

}

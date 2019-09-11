package com.fsnip.spark.ml

import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 13:20 2019/1/23
  * @ Description：主成分分析是一种统计学方法，它使用正交转换从一系列可能相关的变量中提取线性无关变量集，
  * 提取出的变量集中的元素称为主成分。使用PCA方法可以对变量集合进行降维
  * @ Modified By：
  * @ Version:     
  */
object PCATest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("PCATest").master("local[*]").getOrCreate()

    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )

    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(3)//维度必须要<=实际的维度
      .fit(df)

    val result = pca.transform(df).select("pcaFeatures")

    result.show(false)
  }

}

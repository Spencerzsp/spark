package com.fsnip.spark.ml

import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 13:52 2019/1/23
  * @ Description： 多项式扩展通过产生n维组合将原始特征将特征扩展到多项式空间。
  * @ Modified By：
  * @ Version:     
  */
object PolynomialExpansionTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("PCATest").master("local[*]").getOrCreate()

    val data = Array(
      Vectors.dense(-2.0, 2.3),
      Vectors.dense(0.0, 0.0),
      Vectors.dense(0.6, -1.1)
    )

    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val polynomialExpansion = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")
      .setDegree(3)

    val polyDF = polynomialExpansion.transform(df)
    polyDF.select("polyFeatures").take(3).foreach(println)

  }
}

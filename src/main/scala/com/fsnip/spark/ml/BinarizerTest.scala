package com.fsnip.spark.ml

import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 11:23 2019/1/23
  * @ Description：二值化是根据阀值将连续数值特征转换为0-1特征的过程。
                   Binarizer参数有输入、输出以及阀值。特征值大于阀值将映射为1.0，特征值小于等于阀值将映射为0.0
  * @ Modified By：
  * @ Version:     
  */
object BinarizerTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Binarizer").master("local[*]").getOrCreate()

    val data = Array((0, 0.1), (1, 0.8), (2, 0.2), (3, 0.15))

    val dataFrame = spark.createDataFrame(data).toDF("id", "feature")

    val binarizer: Binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(0.15)//设置阈值为0.15，只有当特征值大于该值时才映射为1.0

    val binarizedDataFrame = binarizer.transform(dataFrame)
    binarizedDataFrame.show()
    binarizedDataFrame.select("binarized_feature").collect().foreach(println)
  }
}

package com.fsnip.spark.ml

import org.apache.spark.ml.feature.DCT
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 15:33 2019/5/22
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object DCTExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("DCTExample")
      .master("local[*]")
      .getOrCreate()

    val data = Seq(
      Vectors.dense(0.0, 1.0, -2.0, 3.0),
      Vectors.dense(-1.0, 2.0, 4.0, -7.0),
      Vectors.dense(14.0, -2.0, -5.0, 1.0)
    )

    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    df.show()

    val dct = new DCT()
      .setInputCol("features")
      .setOutputCol("featuresDCT")
      .setInverse(false)

    val dctDF = dct.transform(df)
    dctDF.select("featuresDCT").show(false)

//    val df1 = spark.createDataFrame(
//      Seq(
//        (0.0, 1.0, -2.0, 3.0),
//        (-1.0, 2.0, 4.0, -7.0),
//        (14.0, -2.0, -5.0, 1.0)
//      )
//    ).toDF("features1","features2","features3","features4")
//
//    df1.show()
  }

}

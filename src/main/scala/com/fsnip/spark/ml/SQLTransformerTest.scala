package com.fsnip.spark.ml

import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 14:17 2019/1/23
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object SQLTransformerTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SQLTransformer")
      .master("local[*]")
      .getOrCreate()

    val df = spark.createDataFrame(Seq(

      (0, 1.0, 3.0),
      (2, 2.0, 5.0)

    )).toDF("id", "v1", "v2")

    // 创建临时表
    df.createOrReplaceTempView("V")

    val sqlTrans = new SQLTransformer()
      .setStatement("select *, (v1+v2) as v3, (v1*v2) as v4 from V")

    sqlTrans.transform(df).show()

  }
}

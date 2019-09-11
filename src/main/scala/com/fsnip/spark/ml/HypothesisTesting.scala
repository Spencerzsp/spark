package com.fsnip.spark.ml

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 15:04 2019/1/22
  * @ Description：假设检验测试
  * @ Modified By：
  * @ Version:     
  */
object HypothesisTesting {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Hypothesis Testing")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val data = Seq(

      (0.0, Vectors.dense(0.5, 10.0)),
      (0.0, Vectors.dense(1.5, 20.0)),
      (1.0, Vectors.dense(1.5, 30.0)),
      (0.0, Vectors.dense(3.5, 30.0)),
      (0.0, Vectors.dense(3.5, 40.0)),
      (1.0, Vectors.dense(3.5, 40.0))
    )

//    val df1 = data.map(Tuple1.apply()).toDF("fea")
    val df = data.toDF("label", "features")
    df.show()
//    +-----+----------+
//    |label|  features|
//    +-----+----------+
//    |  0.0|[0.5,10.0]|
//    |  0.0|[1.5,20.0]|
//    |  1.0|[1.5,30.0]|
//    |  0.0|[3.5,30.0]|
//    |  0.0|[3.5,40.0]|
//    |  1.0|[3.5,40.0]|
//    +-----+----------+

    val chi = ChiSquareTest.test(df, "features", "label").head()

    println(s"pValues = ${chi.getAs[Vector](0)}")
    println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
    println(s"statistics ${chi.getAs[Vector](2)}")
  }

}

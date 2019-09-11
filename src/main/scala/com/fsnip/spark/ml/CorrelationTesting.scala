package com.fsnip.spark.ml

import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 17:52 2019/1/21
  * @ Description：相关性矩阵测试
  * @ Modified By：
  * @ Version:     
  */
object CorrelationTesting {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Correlation").master("local[*]")getOrCreate()

    import spark.implicits._ //不导入隐式参数，toDF("features")将报错

    val data = Seq(

      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
      Vectors.dense(4.0, 5.0, 0.0, 3.0),
      Vectors.dense(6.0, 7.0, 0.0, 8.0),
      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
    )
    val df = data.map(Tuple1.apply).toDF("features")

    df.show()
//    +--------------------+
//    |            features|
//    +--------------------+
//    |(4,[0,3],[1.0,-2.0])|
//    |   [4.0,5.0,0.0,3.0]|
//    |   [6.0,7.0,0.0,8.0]|
//    | (4,[0,3],[9.0,1.0])|
//    +--------------------+

    val Row(coeff1: Matrix) = Correlation.corr(df, "features").head

    println(s"Pearson correlation matrix:\n $coeff1")

//    Pearson correlation matrix:
//    1.0                   0.055641488407465814  NaN  0.4004714203168137
//    0.055641488407465814  1.0                   NaN  0.9135958615342522
//    NaN                   NaN                   1.0  NaN
//      0.4004714203168137    0.9135958615342522    NaN  1.0

    val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head()

    println(s"Spearson correlation matrix:\n $coeff2")

//    Spearson correlation matrix:
//    1.0                  0.10540925533894532  NaN  0.40000000000000174
//    0.10540925533894532  1.0                  NaN  0.9486832980505141
//    NaN                  NaN                  1.0  NaN
//      0.40000000000000174  0.9486832980505141   NaN  1.0
  }
}

package com.fsnip.spark.udf

import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 15:59 2019/5/29
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object UDFDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("UDFDemo")
      .master("local[*]")
      .getOrCreate()

    val userData = Array(("Leo", 16), ("Marry", 21), ("Jack", 14), ("Tom", 18))
    val userDF = spark.createDataFrame(userData).toDF("name", "age")

//    注册自定义函数(通过匿名函数)
    val strLen = spark.udf.register("strLen",(str: String) => str.length)
//    注册自定义函数(通过实名函数)
    val udf_isAdult = spark.udf.register("isAdult", isAdult _)

    import org.apache.spark.sql.functions._
    userDF.withColumn("name_len", strLen(col("name"))).withColumn("isAdult", udf_isAdult(col("age"))).show()
    userDF.createOrReplaceTempView("user")

//    spark.sql("select *, strLen(name) as name_len, isAdult(age) as isAdult from user").show()
    spark.sql("select * from user").show()

  }

  def isAdult(age: Int) = {
    if (age < 18)
      false
    else
      true
  }

}

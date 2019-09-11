package com.fsnip.spark.udf

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import org.apache.hadoop.io.Text
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 16:22 2019/5/29
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object Spark2HiveUDF {

  val inputFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
  val outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark2HiveUDF")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

//    spark.sql("show databases").show()

    val time_local_df = spark.sql("select time_local from test.source_log")

    time_local_df.createOrReplaceTempView("date_test")

    val output = new Text()
    spark.udf.register("date_udf",(str: String) => {
      if(str == null)
        null
      val parseDate: Date = inputFormat.parse(str)
      val outputDate = outputFormat.format(parseDate)
      output.set(outputDate)
      output

    })

    spark.sql("select date_udf(time_local_df) from date_test").show()


  }



}

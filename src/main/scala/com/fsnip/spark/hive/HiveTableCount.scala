package com.fsnip.spark.hive

import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 15:02 2019/4/25
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object HiveTableCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("HIveTableCount")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

//    spark.sql("use ods")

    val tables_df = spark.sql("show databases").show()

//    for(i <- tables_df.count())
//      println()

//    tables_df.show()



  }

}

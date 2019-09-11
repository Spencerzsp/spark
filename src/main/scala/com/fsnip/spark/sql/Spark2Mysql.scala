package com.fsnip.spark.sql

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 16:09 2019/5/8
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object Spark2Mysql {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark2Mysql")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

//    读取mysql数据
//    val jdbcDF = spark
//      .read
//      .format("jdbc")
//      .option("url", "jdbc:mysql://DaFa3:3306/test")
//      .option("user", "root")
//      .option("password", "bigdata")
//      .option("dbtable", "table_count")
//      .load()
//
//    jdbcDF.show()

//    读取hive数据进行相关操作并存入mysql

    val hiveDF = spark.sql("select XUUID, XBT, XXCDW from ods. ods_jg_system2jg_xcjlb limit 10")
    hiveDF.show(truncate = false)

    hiveDF.createOrReplaceTempView("test_xcjlb")

    spark.sql("select count(1) count from test_xcjlb").show()

//    hiveDF
//      .write
//      .format("jdbc")
//      .option("url", "jdbc:mysql://DaFa3:3306/test?characterEncoding=utf-8")
//      .option("user", "root")
//      .option("password", "bigdata")
//      .option("dbtable", "test_xcjlb")
//      .mode(saveMode = SaveMode.Overwrite)
//      .save()
  }

}

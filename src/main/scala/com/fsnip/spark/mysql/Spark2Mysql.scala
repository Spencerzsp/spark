package com.fsnip.spark.mysql

import org.apache.spark.sql.SparkSession

object Spark2Mysql {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark2Mysql")
      .master("local[2]")
      .getOrCreate()
    val dataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://wbbigdata00:3306/sewage")
      .option("user", "root")
      .option("password", "bigdata")
      .option("dbtable", "ent_device")
      .option("driver", "com.mysql.jdbc.Driver")
      .load()

    dataFrame.show(truncate = false)
  }

}

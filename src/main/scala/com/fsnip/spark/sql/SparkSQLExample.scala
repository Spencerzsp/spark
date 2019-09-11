package com.fsnip.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 15:33 2019/5/27
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object SparkSQLExample {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkSQLExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val caseClassDS = Seq(Person("Andy",32)).toDS()
    caseClassDS.show()

  }


}

package com.fsnip.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 9:47 2019/4/18
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
case class Record(key: Int, value: String)

object RDDRelation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("RDDRelation")
      .master("local[*]")
      .getOrCreate()

//    import spark.implicits._

//    spark.sqlContext.setConf()

    val df = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))

    df.createOrReplaceTempView("records")

    spark.table("records").show()

//    println("Result of select")
//    spark.sql("select * from records").collect().foreach(println)

//    spark.sql("select count(*) count from records").show()

//    val count = spark.sql("select count(*) from records").collect().head.getLong(0)
//    println(s"count(*): $count")
  }
}

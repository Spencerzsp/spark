package com.fsnip.spark.hive

import java.util
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 17:38 2019/4/17
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */

case class TableCount(id: Int, tableName: String, count: Int, time: String)

object Spark2Hive {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark2Hive")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    spark.sql("show databases").show()
//    spark.sql("select * from fsncloud.ods_fsn_cloud2business_brand limit 10").show()

//    spark.sql("select a.id, a.name, a.status from ods.ods_fsn_cloud2product a limit 10").show()

//    spark.sql("create table test.person(name string, age int, addr string) row format delimited fields terminated by '\t'" )

//    import spark.implicits._
//    val sc = spark.sparkContext
//    val df0 = sc.textFile("/user/root/table_count_merge.txt").map(mapFunctionToTuple4).toDF("id", "table_name", "count", "time")
//    df0.show(50, truncate = false)

//    spark.sql("use ods")
//    val tables = spark.sql("show tables").select("tableName")
//    println(tables.count())

//    tables.createOrReplaceTempView("tables")
//    spark.sql("select * from tables").show()
//    println(tables)
//    println(tables(0)(1))
//    val table_list = List[String]()
//    println(tables.length)
//
//    for(i <- 0 to tables.length-1){
//      println(tables(i)(1))
//      val count_sql = "select count(*) from ods.$table(i)(1)"
//      spark.sql(count_sql).show()
//    }

//    tables.foreach(x => )

//    val tableCount = spark.sql("select id, table_name,")

//    将数据写入mysql，并创建对应的表
//    df0.write.format("jdbc")
//      .option("url", "jdbc:mysql://DaFa3:3306/test")
//      .option("dbtable", "test.table_count")
//      .option("user", "root")
//      .option("password", "bigdata")
//      .mode(saveMode = SaveMode.Overwrite)
//      .save()

//    val url = "jdbc:mysql://sinan3:3306/test"
//    val user = "root"
//    val password = "bigdata"

//    val props = new Properties()
//    props.put("user", user)
//    props.put("password", password)

//    读取mysql数据
//    val df = spark
//      .read
//      .format("jdbc")
//      .options(Map("url" -> url,
//        "user" -> user,
//        "password" -> password,
//        "dbtable" -> "(select * from people limit 10)t",
//        "dbtable" -> "person"
//      )).load().show()

//    val df = spark.read
//      .format("jdbc")
//      .option("url", url )
//      .option("dbtable", "test.table_count")
//      .option("user", user)
//      .option("password", password)
//      .load()
//    df.show()

//    读取hive数据写入mysql
//    spark
//      .read
//      .table("test.person")
//      .write
//      .mode(saveMode = SaveMode.Overwrite)
//      .jdbc(url, "person", props)


//    val df = spark.read.json("/test/test_json")
//    df.show()

    spark.stop()

  }

  def mapFunctionToTuple4(line: String): Tuple4[String, String, String, String]={
    val x = line.split("\t")
    new Tuple4[String, String, String,String](x(0), x(1), x(2), x(3))
  }

}

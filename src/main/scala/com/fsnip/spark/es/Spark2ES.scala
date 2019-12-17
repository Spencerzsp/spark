package com.fsnip.spark.es

import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.sql.EsSparkSQL

object Spark2ES {

  case class StudentInfo(name: String, sex: String, age: Int)
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Spark2ES")
      .master("local[2]")
      .config("es.index.auto.create", "true")
      .config("es.nodes", "wbbigdata00:9200")
      .config("es.nodes.wan.only", "true")
      .config("es.cluster.name", "my-application")
      .getOrCreate()
    val sc = sparkSession.sparkContext

    val rdd = sc.makeRDD(Seq(
      StudentInfo("貂蝉", "女", 34),
      StudentInfo("典韦", "男", 36),
      StudentInfo("曹操", "男", 40)
    ))

    val map = Map("es.mapping.id" -> "name")
//    EsSpark.saveToEs(rdd, "/student/type", map)
//    printf("============RDD写入ES成功！！！=================")

    val resultRdd = EsSpark.esRDD(sc, "/student/type")
//    resultRdd.foreach(println)

    val df = sparkSession.createDataFrame(sc.parallelize(Seq(
      StudentInfo("小明", "男", 18),
      StudentInfo("小红", "女", 19),
      StudentInfo("小放", "女", 20)
    ))).toDF("name", "sex", "age")
    EsSparkSQL.saveToEs(df, "/student/type", map)

    val esQuery =
      """
        |{
        | "query": {
        |   "match": {
        |       "sex": "男"
        |   }
        |  }
        |}
      """.stripMargin

    val frame = EsSparkSQL.esDF(sparkSession, "/student/type", esQuery)
    frame.show()
  }

}

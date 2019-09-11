package com.fsnip.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 14:57 2019/7/4
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object RDDJoin {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RDDJoin").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(Array(("zhangsan", 10), ("lisi", 20), ("wangwu", 30), ("maliu", 40)),3)

    val rdd2 = sc.makeRDD(Array(("zhangsan", 100), ("lisi", 200), ("wangwu", 300)))

//    rdd1.re

//    rdd1.join(rdd2).foreach(println(_))
    rdd2.leftOuterJoin(rdd1).foreach(println)

    rdd1.mapPartitions(iter => {
      println("插入数据")
      iter
    }, true).collect()

    rdd1.foreachPartition(iter =>{
      iter.foreach(println(_))
    })
  }

}

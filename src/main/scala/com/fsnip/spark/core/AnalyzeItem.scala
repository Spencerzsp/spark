package com.fsnip.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 16:08 2019/9/27
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object AnalyzeItem {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("AnalyzeItem")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    val lines = sc.textFile("file:///F:\\scala-workspace\\spark\\data\\item.txt")
    val data = lines.map(_.split(","))
      .map(purchaseRecord => (purchaseRecord(0), purchaseRecord(1), purchaseRecord(2)))

    println(data)

    // 求购买次数
    val numPurchases = data.count()
    // 求有多少个不同客户购买过商品
//    val uniqueUsers = data.map{case (user, product, price) => user}.distinct().count()
    val uniqueUsers2 = data.map(item => item._1).distinct().count()
    // 求和得出总收入
//    val sumPrices = data.map{case(user, product, price) => price.toDouble}.sum()
    val sumPrices2 = data.map(item => item._3.toDouble).sum()
    // 求最畅销的产品是什么
    val productsByPopularity = data.map{item => (item._2, 1)}
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
    val mostPopular = productsByPopularity.first()

    println("Total purchases: " + numPurchases)
    println("Unique users: " + uniqueUsers2)
    println("Total revenue: " + sumPrices2)
    println("Most popular product: %s with %d purchases".format(mostPopular._1, mostPopular._2))
  }

}

//case class Item(user: String, product: String, price: String)

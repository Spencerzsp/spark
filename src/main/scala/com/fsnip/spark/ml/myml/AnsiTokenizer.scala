package com.fsnip.spark.ml.myml

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 13:44 2019/7/17
  * @ Description：ansj分词器
  * @ Modified By：
  * @ Version:     
  */
object AnsiTokenizer {
  def main(args: Array[String]): Unit = {

    val spark= SparkSession
      .builder()
      .appName("TextClassify")
      .master("local[*]")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val filter = new StopRecognition()
//    过滤掉标点
    filter.insertStopNatures("w")
    val rdd = spark.sparkContext.textFile("file:///F:\\scala-workspace\\spark\\data\\words\\word.txt")
      .map(line => {
        val str = if(line.length > 0){
          ToAnalysis.parse(line).recognition(filter).toStringWithOutNature(" ")
        }
        str.toString
      })

    rdd.saveAsTextFile("/user/test1/output/words2")




  }

}


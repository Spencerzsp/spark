package com.fsnip.flink

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 9:50 2019/9/12
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object WordCountBatch {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile("hdfs://DaFa3:8020/user/test2/word.txt")

    import org.apache.flink.api.scala._
    val counts = text.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    counts.print()


  }
}

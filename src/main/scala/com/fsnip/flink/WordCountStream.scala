package com.fsnip.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 9:37 2019/9/12
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object WordCountStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("DaFa1", 19888)

    import org.apache.flink.api.scala._
    val counts = text.flatMap(_.toLowerCase.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    counts.print()
    env.execute("基于scala的flink流式统计单词")

  }
}

package com.fsnip.spark.hbase

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 9:57 2019/7/23
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object SparkWriteHBase2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkWriteHBase1")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    Logger.getRootLogger.setLevel(Level.WARN)

    val tableName = "student"
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val job = Job.getInstance(sc.hadoopConfiguration)
//    val job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val initDataRDD = sc.makeRDD(Array("row01,jack,15","row01,Lily,16","row01,mike,16"))
    val rdd = initDataRDD.map(_.split(",")).map(arr => {
      val put = new Put(Bytes.toBytes(arr(0)))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("age"),Bytes.toBytes(arr(2).toInt))
      (new ImmutableBytesWritable, put)
    })

    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())

    sc.stop()
  }

}

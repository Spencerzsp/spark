package com.fsnip.spark.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 13:42 2019/7/23
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object SparkWriteHBase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkWriteHBase1")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = HBaseConfiguration.create()
    val tableName = "student"

    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val initDataRDD = sc.makeRDD(Array("row03,jack,15","row04,Lily,16","row05,mike,16", "row06,spencer,20"))
    val rdd = initDataRDD.map(_.split(",")).map(arr =>{
      val put = new Put(Bytes.toBytes(arr(0)))
      val cf = Bytes.toBytes("cf")
      put.addColumn(cf, Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
      put.addColumn(cf, Bytes.toBytes("age"), Bytes.toBytes(arr(2)))
      (new ImmutableBytesWritable(), put)
    })

    rdd.saveAsHadoopDataset(jobConf)

    sc.stop()
  }

}

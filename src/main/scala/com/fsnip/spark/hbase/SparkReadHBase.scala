package com.fsnip.spark.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 17:51 2019/5/8
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object SparkReadHBase {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Spark2Hbase").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val conf = HBaseConfiguration.create()

//    conf.set("hbase.zookeeper.quorum","sinan3")
//    conf.set("hbase.zookeeper.property.clientPort", "2181")
//    conf.set("zookeeper.session.timeout", "600000")
//    conf.setInt("hbase.rpc.timeout", 20000)
//    conf.setInt("hbase.client.operation.timeout", 30000)
//    conf.setInt("hbase.client.scanner.timeout.period", 200000)

    val tableName = "student"
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

//    如果表不存在则创建表

//    val admin = new HBaseAdmin(conf)
//    if(!admin.isTableAvailable(tableName)){
//      val tableDec = new HTableDescriptor(TableName.valueOf(tableName))
//      admin.createTable(tableDec)
//    }

//    读取数据并转换为RDD
    val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = hbaseRDD.count()
    println(count)

//    hbaseRDD.foreach(println)

    hbaseRDD.foreach{case (_, result) => {
      //获取行键
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列,且必须全部将字节转换成字符串
      val id = Bytes.toString(result.getValue("cf".getBytes,"id".getBytes))
      val name = Bytes.toString(result.getValue("cf".getBytes,"name".getBytes))
      val age = Bytes.toString(result.getValue("cf".getBytes,"age".getBytes))
      val address = Bytes.toString(result.getValue("cf".getBytes,"addr".getBytes))
      println("Row key:"+key+" Name:"+name+" Age:"+age + " ID:"+id + " Addr:" + address)
    }}

    sc.stop()
//    admin.close()


  }

}

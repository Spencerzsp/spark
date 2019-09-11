package com.fsnip.spark.ml

import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.SparkSession

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 14:57 2019/1/25
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object RFormula {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("RFormula").master("local[*]").getOrCreate()

    val dataset = spark.createDataFrame(Seq(

      (7, "US", 18, 1.0),
      (8, "CA", 12, 0.0),
      (9, "NZ", 15, 0.0)

    )).toDF("id", "country", "hour", "clicked")

    dataset.select("id", "country", "hour", "clicked").show(false)

    //当需要通过country和hour来预测clicked时候，
    //构造RFormula，指定Formula表达式为clicked ~ country + hour
    val formula = new RFormula()
      .setFormula("clicked ~ country + hour")
      .setFeaturesCol("features")
      .setLabelCol("label")

    val output = formula.fit(dataset).transform(dataset)

    output.select("id", "country", "hour", "clicked", "features", "label").show(false)
  }
}

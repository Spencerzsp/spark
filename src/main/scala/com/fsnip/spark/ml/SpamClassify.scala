package com.fsnip.spark.ml

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author     ：zsp
  * @ Date       ：Created in 17:30 2019/1/24
  * @ Description：
  * @ Modified By：
  * @ Version:     
  */
object SpamClassify {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("SpamClassify").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val spam = sc.textFile("/test/spam.txt")

    val normal = sc.textFile("/test/normal.txt")

    val tf = new HashingTF()

    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))

    val normalFeatures = normal.map(email => tf.transform(email.split(" ")))

    // 创建LabeledPoint数据集分别存放阳性（垃圾邮件）和阴性（正常邮件）的例子
    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))

    val negativeExamples = normalFeatures.map(features => LabeledPoint(0, features))

    val trainingData = positiveExamples.union(negativeExamples)

    trainingData.cache() // 因为逻辑回归是迭代算法，所以缓存训练数据RDD

    // 使用SGD算法运行逻辑回归
    val model = new LogisticRegressionWithLBFGS().run(trainingData)

    // 以阳性（垃圾邮件）和阴性（正常邮件）的例子分别进行测试

    val posTest = tf.transform(
      "O M G GET cheap stuff by sending money to ...".split(" "))

    val negTest = tf.transform(
      "Hi Dad, I started studying Spark the other ...".split(" "))

    println("Prediction for positive test example: " + model.predict(posTest))

    println("Prediction for negative test example: " + model.predict(negTest))

    Thread.sleep(100000)

    sc.stop()
  }

}

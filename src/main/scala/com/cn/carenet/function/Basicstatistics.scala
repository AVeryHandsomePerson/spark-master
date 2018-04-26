package com.cn.carenet.function

import org.apache.spark.mllib.linalg.{Matrix, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.SparkSession

object Basicstatistics {
  /**一.基本统计量
    * 统计向量的长度,最大值，最小值，非0个数，模1和模2,方差等
    */
  def statistics(): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("Basicstatistics").getOrCreate()
    val data = spark.sparkContext.parallelize(1 to 100)

    val obs=data.map(x=>Vectors.dense(x))
    val summary=Statistics.colStats(obs)

    println(summary.mean)
    println(summary.max)
    println(summary.min)
  }

  /**
    * 相关系数
    * 无	−0.09 to 0.0	0.0 to 0.09
    * 弱	−0.3 to −0.1	0.1 to 0.3
    * 中	−0.5 to −0.3	0.3 to 0.5
    * 强	−1.0 to −0.5	0.5 to 1.0
    *
    */
  def Correlationcoefficient(): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("Basicstatistics").getOrCreate()
    val arr3 = (1 to 5).toArray.map(_.toDouble)
    val arr4 = (11 to 15).toArray.map(_.toDouble)
    val rdd7 = spark.sparkContext.parallelize(Array(170.0, 150.0, 210.0,180.0,160.0))
    val rdd8 = spark.sparkContext.parallelize(Array(230.0, 220.0, 210.0,240.0,250.0))
    val rdd3 = spark.sparkContext.parallelize(arr3)
    val rdd4 = spark.sparkContext.parallelize(arr4)
    println(arr3.mkString(","))
    println(arr4.mkString(","))
    val correlation: Double = Statistics.corr(rdd7, rdd8)//计算不同数据之间的相关系数:皮尔逊
    val correlations: Double = Statistics.corr(rdd7, rdd8,"spearman")//使用斯皮尔曼计算不同数据之间的相关系数

    println(correlation)
    println("correlationSpearman：" + correlations) //打印结果
    //    val data=spark.sparkContext.textFile("coo1").map(_.split(",")).map(_.map(_.toDouble)).map(x=>Vectors.dense(x))
    //
    //    val correlMatrix: Matrix = Statistics.corr(data, "pearson")
    //    correlMatrix
  }

  /**
    * 分成抽样
    * RDD有个操作可以直接进行抽样：sampleByKey和sample等，这里主要介绍这两个
    * 将字符串长度为2划分为层1和层2，对层1和层2按不同的概率进行抽样
    *
    * fractions表示在层1抽0.2，在层2中抽0.8
    * withReplacement false表示不重复抽样
    * 0表示随机的seed
    */
  def DividedIntoSamples(): Unit ={
    val spark = SparkSession.builder().master("local[*]").appName("Basicstatistics").getOrCreate()
    val randRDD = spark.sparkContext.parallelize(List((7, "cat"), (6, "mouse"), (7, "cup"), (6, "book"), (7, "tv"), (6, "screen"), (7, "heater")))
    val sampleMap = List((7, 0.4), (6, 0.5)).toMap  //设定抽样格式
    val sample2 = randRDD.sampleByKey(false, sampleMap,0).collect//计算抽样样本
    sample2.foreach(println)

    println("Third:")
    //http://bbs.csdn.net/topics/390953396
    val a = spark.sparkContext.parallelize(1 to 20, 3)
    val b = a.sample(true, 0.8, 0)
    val c = a.sample(false, 0.8, 0)
    println("RDD a : " + a.collect().mkString(" , "))
    println("RDD b : " + b.collect().mkString(" , "))
    println("RDD c : " + c.collect().mkString(" , "))
  }

  def main(args: Array[String]): Unit = {

  }
}

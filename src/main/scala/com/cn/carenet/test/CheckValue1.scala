package com.cn.carenet.test

import org.apache.spark.{SparkConf, SparkContext}
object CheckValue1 {
  def main(args: Array[String]) {

    //创建入口对象
    val conf = new SparkConf().setAppName("CheckValue1").setMaster("local")
    val sc= new SparkContext(conf)
    val HDFS_DATA_PATH = "hdfs://node1:9000/user/spark/sparkLearning/cluster/kddcup.data"
    val rawData = sc.textFile(HDFS_DATA_PATH)

    /**
      * 实验一
      * 分类统计样本个数，降序排序
      */
    val sort_result = rawData.map(_.split(",").last).countByValue().toSeq.sortBy(_._2).reverse
    sort_result.foreach(println)
    //    程序结果运行如下：
    //          (smurf.,2807886)
    //          (neptune.,1072017)
    //          (normal.,972781)
    //          (satan.,15892)
    //          (ipsweep.,12481)
    //          (portsweep.,10413)
    //          (nmap.,2316)
    //          (back.,2203)
    //          (warezclient.,1020)
    //          (teardrop.,979)
    //          (pod.,264)
    //          (guess_passwd.,53)
    //          (buffer_overflow.,30)
    //          (land.,21)
    //          (warezmaster.,20)
    //          (imap.,12)
    //          (rootkit.,10)
    //          (loadmodule.,9)
    //          (ftp_write.,8)
    //          (multihop.,7)
    //          (phf.,4)
    //          (perl.,3)
    //          (spy.,2)

  }
}

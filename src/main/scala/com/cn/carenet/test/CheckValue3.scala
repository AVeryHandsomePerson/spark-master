package com.cn.carenet.test

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
object CheckValue3 {
  def main(args: Array[String]) {
    //创建入口对象
    val conf = new SparkConf().setAppName("CheckValue1").setMaster("local")
    val sc= new SparkContext(conf)
    val HDFS_DATA_PATH = "hdfs://node1:9000/user/spark/sparkLearning/cluster/kddcup.data"
    val rawData = sc.textFile(HDFS_DATA_PATH)

    val LabelsAndData = rawData.map{   //代码块执行RDD[String] => RDD[Vector]
      line =>
        //toBuffer创建一个可变列表(Buffer)
        val buffer = line.split(",").toBuffer
        buffer.remove(1, 3)
        val label = buffer.remove(buffer.length-1)
        val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
        (label, vector)
    }
    val data = LabelsAndData.values.cache()  //转化值并进行缓存

    //建立kmeansModel
    val kmeans = new KMeans()
    val model = kmeans.run(data)

    /**由CheckValue1已知该数据集有23类，CheckValue2分类肯定不准确，所以下面我利用给定的类别标号信息来
      *直观的看到分好的簇中包含哪些类型的样本，对每个簇中的标号进行计数，并以可读的方式输出
      */
    //对标号进行计数
    val clusterLabelCount = LabelsAndData.map {
      case (label, datum) =>
        val cluster = model.predict(datum)
        (cluster, label)
    }.countByValue()
    //将簇-类别进行计数，输出
    println("计数结果如下")
    clusterLabelCount.toSeq.sorted.foreach {
      case ((cluster, label), count) =>
        //使用字符插值器对变量的输出进行格式化
        println(f"$cluster%1s$label%18s$count%8s")
    }

    //    计数结果如下
    //    0             back.    2203
    //    0  buffer_overflow.      30
    //    0        ftp_write.       8
    //    0     guess_passwd.      53
    //    0             imap.      12
    //    0          ipsweep.   12481
    //    0             land.      21
    //    0       loadmodule.       9
    //    0         multihop.       7
    //    0          neptune. 1072017
    //    0             nmap.    2316
    //    0           normal.  972781
    //    0             perl.       3
    //    0              phf.       4
    //    0              pod.     264
    //    0        portsweep.   10412
    //    0          rootkit.      10
    //    0            satan.   15892
    //    0            smurf. 2807886
    //    0              spy.       2
    //    0         teardrop.     979
    //    0      warezclient.    1020
    //    0      warezmaster.      20
    //    1        portsweep.       1
  }
}

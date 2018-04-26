package com.cn.carenet.test

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
object CheckValue5 {
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
    //设置给定k值的运行次数
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    (30 to 100 by 10).par.map(k => (k, CountClass.clusteringScore(data, k))).toList.foreach(println)

    //    程序运行如下:
    //          (30,584.3824466136748)
    //          (40,473.13630965059355)
    //          (50,397.1680468789708)
    //          (60,224.7032729131013)
    //          (70,209.75091102083454)
    //          (80,189.24155085526263)
    //          (90,192.57698780271707)
    //          (100,119.81903683729702)

    /**
      * 总结：随着k的增大，结果得分持续下降，我们要找到k值的临界点，过了这个临界点之后继续增加k值并不会显著降低得分
      * 这个点的k值-得分曲线的拐点。这条曲线通常在拐点之后会继续下行但最终趋于水平。
      * 在本实例中k>100之后得分下降很明显，故得出k的拐点应该大于100
      *
      * internet包下的代码主要目的是做个网络流量异常的检测的实验
数据集是基于KDD Cup1999数据集建立生产系统，举办方已经对这个数据集(网络流量包)进行了加工



         1、CheckValue1是对数据集里面的样本进行统计和分类，按降序排序

         2、CheckValue2是对数据集里面的样本进行建立KMeansModel，在输出每个簇的质心

         3、CheckValue3是对数据集里面的样本中的每个簇中每个标号出现的次数进行计数，按照簇-类别计数，并且按照可读的方法输出

         4、CheckValue4是对数据集里面的样本进行对k的取值进行评价

         5、CheckValue5是对数据集进行再次k的取值然后进行评价操作，来大概统计出k的最佳值在什么区间，这里用到的是利用k值进行多次聚类操作，
            利用setRuns()方法给定k的运行次数

         6、CountClass单例对象里面封装了欧氏距离公式和将欧氏距离应用到model中，还有计算k值model平均质心距离和对k的取值进行评价的方法

         说明：这个小项目主要完成的是对网络流量异常的检测
      */


  }
}

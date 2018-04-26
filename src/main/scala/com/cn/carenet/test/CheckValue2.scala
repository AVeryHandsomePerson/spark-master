package com.cn.carenet.test

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
object CheckValue2 {
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
    model.clusterCenters.foreach(println)

    //    程序运行结果：
    //          向量1:
    //          [48.34019491959669,1834.6215497618625,826.2031900016945,5.7161172049003456E-6,
    //          6.487793027561892E-4,7.961734678254053E-6,0.012437658596734055,
    //          3.205108575604837E-5,0.14352904910348827,0.00808830584493399,
    //          6.818511237273984E-5,3.6746467745787934E-5,0.012934960793560386,
    //          0.0011887482315762398,7.430952366370449E-5,0.0010211435092468404,
    //          0.0,4.082940860643104E-7,8.351655530445469E-4,334.9735084506668,
    //          295.26714620807076,0.17797031701994342,0.1780369894027253,
    //          0.05766489875327374,0.05772990937912739,0.7898841322630883,
    //          0.021179610609908736,0.02826081009629284,232.98107822302248,
    //          189.21428335201279,0.7537133898006421,0.030710978823798966,
    //          0.6050519309248854,0.006464107887636004,0.1780911843182601,
    //          0.17788589813474293,0.05792761150001131,0.05765922142400886]
    //          向量2:
    //          [10999.0,0.0,1.309937401E9,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
    //          0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,1.0,0.0,0.0,1.0,1.0,1.0,
    //          0.0,0.0,255.0,1.0,0.0,0.65,1.0,0.0,0.0,0.0,1.0,1.0]
  }
}

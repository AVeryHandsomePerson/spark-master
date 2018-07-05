package com.cn.carenet.function

import java.util
import java.util.TreeSet

import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

object SparkSqlTest {
  /**
    * Spark2 Dataset DataFrame空值null,NaN判断和处理
    */
  def sparkNulldealwith: Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("SparkSqlTest").getOrCreate()
    val path = "shakespeare/line"
    val options = Map("pushdown" -> "true","es.nodes" -> "192.168.6.113","es.port" -> "9200")
    val sparkDF = spark.read.format("org.elasticsearch.spark.sql").options(options).load(path)
    // 删除所有列的空值和NaN
    sparkDF.na.drop()
    //删除某列的空值和NaN
    sparkDF.na.drop(Array("gender","yearsmarried"))
    //填充所有空值的列
    sparkDF.na.fill("")
    //对指定的列空值填充
    sparkDF.na.fill(value="wangxiao111",cols=Array("gender","yearsmarried"))
    sparkDF.na.fill(Map("gender"->"wangxiao111","yearsmarried"->"wangxiao111"))
    //查询空值列
    sparkDF.filter("gender is null").select("gender").limit(10).show
    sparkDF.filter("gender is not null").select("gender").limit(10).show
    sparkDF.filter(sparkDF("gender is not null").isNull).select("gender").limit(10).show
  }


  def main(args: Array[String]): Unit = {
//    val filePath = args()
    val spark = SparkSession.builder().master("local[*]").appName("SparkSqlTest").getOrCreate()
//    val broadType = spark.sparkContext.broadcast(Set(1, 2, 3, 4, 5, 6, 7))


    val  str = spark.sparkContext.textFile("C:\\Users\\dell\\Desktop\\移(1)\\data.tar.gz")
//    str.map(x => x.split(" ")).foreach(x => if (x.length > 2) println(x(0)))





//
//    val map = Map(1 -> 2,3 -> 4)
//    val math = Array(3,2,34,5)
//    val ma = spark.sparkContext.parallelize(math).map(x =>(x,1)).collect().toMap
//
//    val br =spark.sparkContext.broadcast(ma)
//
//
//    println(br.value.getOrElse(1,""))

//   val mapBroad =  spark.sparkContext.broadcast(map)
//    println(mapBroad.value.getOrElse(1,-1))








    val treeadd: util.TreeSet[Long] = new util.TreeSet[Long]
    treeadd.add(20180701001738L)
    treeadd.add(20180701004020L)
    treeadd.add(20180701012546L)
    treeadd.add(20180701025633L)
    treeadd.add(20180701034204L)
    treeadd.add(20180701042732L)
    treeadd.add(20180701042732L)
    treeadd.add(20180701044921L)
    treeadd.add(20180701051202L)
    treeadd.add(20180701001738L)
    treeadd.add(20180701004020L)
    treeadd.add(20180701010302L)
    treeadd.add(20180701014823L)
    treeadd.add(20180701053446L)
    treeadd.add(20180701044921L)
    treeadd.add(20180701040444L)
    treeadd.add(20180701023351L)
    treeadd.add(20180701021103L)
    treeadd.add(20180701021103L)
    treeadd.add(20180701012546L)
    treeadd.add(20180701010302L)
    treeadd.add(20180701053446L)
    treeadd.add(20180701051202L)
    treeadd.add(20180701040444L)
    treeadd.add(20180701034204L)
    treeadd.add(20180701031920L)
    treeadd.add(20180701025633L)
    treeadd.add(20180701023351L)
//    val treeadds: util.TreeSet[Int] = new util.TreeSet[Int]
//    treeadds.addAll(treeadd)
//    println(broadType.value(2))
//    20180701004020
    println(treeadd.higher(20180701004020L))
//    println(treeadd.lower(16))

  }
}

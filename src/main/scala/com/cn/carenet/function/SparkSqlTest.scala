package com.cn.carenet.function

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


  }
}

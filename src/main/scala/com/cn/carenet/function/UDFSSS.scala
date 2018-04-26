package com.cn.carenet.function

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

object UDFSSS {
  //自定义udf函数
  def Customfunction(): Unit ={
    val spark = SparkSession.builder().master("local[*]").appName("Udfss").getOrCreate()
    val path = "shakespeare/line"
    val options = Map("pushdown" -> "true","es.nodes" -> "192.168.6.113","es.port" -> "9200")
    val sparkDF = spark.read.format("org.elasticsearch.spark.sql").options(options).load(path)
    //    val str =DataTypes.createStructField("str", DataTypes.StringType, true)
    //    str
    sparkDF.createOrReplaceTempView("abc")
    spark.sqlContext.udf.register("stringCount", new UserDefinedAggregateFunction() {
      // 指定输入数据的字段与类型
      override def inputSchema: StructType = {
        val str =new Array[StructField](1)
        str(0) =DataTypes.createStructField("str", DataTypes.StringType, true)
        DataTypes.createStructType(str)
      }
      // 聚合操作时，所处理的数据的类型
      // 即 buffer中数据的schema，buffer就是一个Row
      override def bufferSchema: StructType = {
        val str =new Array[StructField](1)
        str(0) =DataTypes.createStructField("bf", DataTypes.IntegerType, true)
        DataTypes.createStructType(str)
      }
      // 最终函数返回值的类型
      override def dataType: DataType = {
        DataTypes.IntegerType
      }

      /**
        * whether given the same input,
        * always return the same output
        * true: yes
        * 6217858000092146312
        */
      override def deterministic: Boolean = {
        true
      }
      /**
        * buffer就是一个Row
        * buffer初始化 可以认为是，你自己在内部指定一个初始的值
        */
      override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer.update(0, 0)
      }
      /**
        * 更新 可以认为是，一个一个地将组内的字段值传递进来 实现拼接的逻辑
        * buffer.getInt(0)获取的是上一次聚合后的值
        * 相当于map端的combiner，combiner就是对每一个map task的处理结果进行一次小聚合
        *    大聚和发生在reduce端
        */
      override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        // 更新0号位置值
        buffer.update(0, buffer.getInt(0) + 1);
      }
      /**
        * 合并 update操作，可能是针对一个分组内的部分数据，在某个节点上发生的 但是可能一个分组内的数据，会分布在多个节点上处理
        * 此时就要用merge操作，将各个节点上分布式拼接好的串，合并起来
        * buffer1.getInt(0) : 大聚和的时候 上一次聚合后的值
        * buffer2.getInt(0) : 这次计算传入进来的update的结果
        */
      override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = buffer1.update(0, buffer1.getInt(0) + buffer2.getInt(0))

      override def evaluate(buffer: Row): Any = {
        Row(0)
      }
    } )
    spark.sqlContext.sql("SELECT province,stringCount(province) from abc group by 'province'").show
  }
  def main(args: Array[String]): Unit = {
    Customfunction()
  }
}

package com.cn.carenet.HX

import java.util.Properties

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.{SaveMode}
import redis.clients.jedis.Jedis

object SDYd {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("SDYd").getOrCreate()

   val str : Dataset[String] = spark.read.textFile("C:\\Users\\dell\\Desktop\\移(1)\\data.tar.gz")
    import spark.implicits._
    str.map(_.split("\\|",-1)).filter(_.length >8).map {
      data =>
        data(0) match {
          case "08" =>
            SDSourceTmp(
              state = data(0),
              platform = data(1),
              user_id = data(2),
              start_time = data(5),
              oneUrl = data(6),
              afterUrl = data(7)
            )
          case _ =>
            SDSourceTmp(
              state = data(0)
            )
        }
    }.filter(_.state.equals("08")).createOrReplaceTempView("b")
    //164667
    val prop =new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","root")


    spark.sql("select oneUrl,count(distinct(user_id)) as uv from b where platform = 'ZTE' group by oneUrl ")
      .write.mode(SaveMode.Overwrite)
      .jdbc("jdbc:mysql://localhost:3306/flie","t_oneUrl_count",prop)
    spark.stop()




//    sim_follow.foreachPartition(it => {
//      val jedis = new Jedis("xxx",6379)
//      for(tuple <- it){
//        val key = "xxx" + tuple._1
//        val value = tuple._2.toString
//        jedis.set(key,value)
//      }
//    })
//
//    str.map(_.split("|",-1)).filter("").show()

  }
}

package com.cn.carenet.function

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkFile {
  def DateFormat(time: String): Long = {
    var sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddhhmmss")
    var date: String = sdf.format(new Date((time.toLong * 1000l)))
    date.toLong
  }
  def myFun(iterator:(String, String, String)): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into blog(id,name,yName) values (?,?,?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark","root", "root")
        ps = conn.prepareStatement(sql)
        ps.setString(1, iterator._1)
        ps.setString(2, iterator._2)
        ps.setString(2, iterator._3)
        ps.executeUpdate()
    } catch {
      case e: Exception => println("Mysql Exception")
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("SparkFile").getOrCreate()

    //    var begin:Date = df.parse("20180507144912")
    //    var end:Date  = df.parse("20180507151339")
    //    println(begin)
    //    println(end)
    //    var between:Long=(end.getTime()-begin.getTime())/1000//转化成秒
    //    var hour:Float=between.toFloat/60
    //    println(hour)


    val str = spark.sparkContext.textFile("D:\\IDEARFILE\\aaaaa\\file\\2018.06.26.10.evt")
    val sse = str.cache()
//    str.take(10).foreach(println(_))
    var df: SimpleDateFormat = new SimpleDateFormat("yyyyMMddhhmmss")
    val timeDifference = sse.map(x => (x.split("\\|")(1), x.split("\\|")(3))).groupBy(_._1).map(x => x._2.toList.sorted)
//    timeDifference.foreach(x => println((df.parse(x(x.length - 1)._2).getTime - df.parse((x(0)._2)).getTime).toFloat / 60000))

    //获得 订购用户
   val fileUse = sse.map(x => {
     var id = ""
     var yw = ""
     var gTime = ""
     if(x.split("\\|").length >= 21){
       yw = x.split("\\|")(0)
       id = x.split("\\|")(2)
       gTime = x.split("\\|")(5)
      }
      (yw,id,gTime)
    }).filter(_._1.equals("05")).distinct().collect()
    //根据订购用户 获取 订购用户 时间 影片名称 操作影片时长
   val kName = sse.map(x => {
           var yw = ""
           var id = ""
           var gTime = ""
           var yName = ""
//           var jmOp = ""
      if(x.split("\\|").length >=18){
        id = x.split("\\|")(2)
        yw = x.split("\\|")(0)
        gTime = x.split("\\|")(5)
        yName = x.split("\\|")(10)
//        jmOp  = x.split("\\|")(16)
      }
     if(gTime.isEmpty){
       gTime = "0"
     }
     (yw,id,gTime.toLong,yName)
    }).filter(_._1.equals("02"))
    val se =kName.cache()
//    for (i <- 0 until(3)){
//     se.filter(_._2.equals(fileUse(i)._2))
//        .filter(x => df.parse(x._3).getTime > df.parse(fileUse(i)._3).getTime)
//        .foreach(println(_))
//    }

    for (i <- 0 until(2)){
      //根据用户 ID  订购时间 找出用户订购后 观看的第一部电影
      val st = se.filter(_._2.equals(fileUse(i)._2))
        .filter(x => df.parse(x._3.toString).getTime > df.parse(fileUse(i)._3).getTime)
      // 取到 第一时间观看的电影 名字
     val str = st
        .sortBy(x => x._3)//20180626110423
        .take(1)
//      val yName = str(0)._4
//      println(yName)
//      (02,053902236772,20180626110201,20180429期：沈月于文文拍照大赛笑料不断,1X)
//       取到 第一时间观看的电影的结束时间
      val endTime = st.filter(x => x._4.contains(str(0)._4)).top(1)


//    str.foreach(println(_))
//      if(endTime(0)._3 - str(0)._3 > 6){
//        println(str(0).toString())
//      }

//        .sortBy(x => x._3,false)//20180626110423
//
//      endTime.foreach(println(_))
      // 拿结束时间 减去开始时间 得出 影片观看时长
     val s = str.filter(x => (df.parse(endTime(0)._3.toString).getTime - df.parse(x._3.toString).getTime).toFloat / 60000 <= 6)
           .map(x => (x._1,x._2,x._4)).foreach(myFun)

//      (df.parse(endTime(0)._3.toString).getTime - df.parse(x._3.toString).getTime).toFloat / 60000
//        .top(1)
//        .foreach(x => println())
//      20180626110423
    }
//

//    val a = spark.sparkContext.parallelize(1 to 9, 3)
//    def doubleFunc(iter: Iterator[Int]) : Iterator[(Int,Int)] = {
//      var res = List[(Int,Int)]()
//      while (iter.hasNext)
//      {
//        val cur = iter.next;
//        res .::= (cur,cur*2)
//      }
//      res.iterator
//    }
//    val results = a.mapPartitions(doubleFunc)
//    val result = a.mapPartitions(doubleFunc)
//    println(results.collect().mkString)
//    println(result.collect().mkString)




  }
}

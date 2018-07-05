package com.cn.carenet.HX


import java.text.SimpleDateFormat
import org.apache.spark.sql.{SaveMode, SparkSession}

/***
  * 1：需求
  *   统计订购用户，在订购后观看第一步影片时长超过6分钟的电影ID和影片名称
  * 2：业务分析
  *   1. 取出订购用户
  *   2. 取出订购用户中的点播用户
  *   3. 根据订购时间取出点播用户中在订购之后的观看用户和观看时间,观看影片名称
  *   4. 取出订购后第一个观看的电影，之后对该电影时间排序取出开始时间和结束时间
  *   5. 对影片开始时间和结束时间求差 算出 观看超过6分钟的电影和 用户
  *3：字段需求(下标从0开始)
  *   订购表05 字段：
  *     1.业务类型（0） 2.用户ID（2）3.自然时间（5）
  *   点播表02 字段：
  *     1.业务类型（0） 2.用户ID（2）3.自然时间（5）4.VOD影片名称（10）
  *
  *
  */
object DGFilm {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("DGFilm").getOrCreate()
//    val str = spark.read.textFile("/source/yd/sdyd/2018.06.26.19.evt")
    val str = spark.read.textFile("C:\\Users\\dell\\Desktop\\移(1)\\移\\2018.06.26.19.evt")
    //连接mysql
//    val prop =new Properties()
//    prop.setProperty("user","root")
//    prop.setProperty("password","root")


    import spark.implicits._
    str.map(_.split("\\|",-1)).filter(_.length > 4).map(data =>{
      data(0) match {
        case "05" =>
          DBSourceUser(
            ywType = data(0),
            userID = data(2),
            zTime = data(5).toLong
          )
        case "02" =>
          DBSourceUser(
            ywType = data(0),
            userID = data(2),
            zTime = data(5).toLong,
            filmName = data(10)
          )
        case _ =>
          DBSourceUser(
            ywType = "001",
            userID = data(2)
          )
      }
    }).filter(!_.ywType.equals("001")).createOrReplaceTempView("b")

    spark.sql("select * from b where ywType='05'").createOrReplaceTempView("order")
    spark.sql("select * from b where ywType='02'").createOrReplaceTempView("watch")
    //取出订单后 观看影片的 用户和电影名称和时间
    spark.sql("select distinct(watch.userID),watch.zTime,watch.filmName from watch,order " +
      "where order.userID = watch.userID and watch.zTime < order.zTime ").createOrReplaceTempView("c")
    spark.sqlContext.cacheTable("c")
    //取出订单后第一步电影时间
    spark.sql("select userID,min(zTime) as zTime from c group by  userID ").createOrReplaceTempView("g")
    //取出第一时间观看的电影名称
    spark.sql("select c.userID as userID,c.filmName,g.zTime from c,g where c.zTime= g.zTime ").createOrReplaceTempView("h")
    spark.sqlContext.cacheTable("h")
//    spark.sql("select min(zTime),max(zTime),filmName,userID from c group by  userID,filmName").show(false)
    //取出第一时间观看的电影的结束时间
    spark.sql("select max(c.zTime) as zTime,h.filmName from c,h where c.filmName=h.filmName group by h.filmName").createOrReplaceTempView("e")
    //将时间转换成毫秒
    spark.udf.register("data_new",func =(str:Long) =>{
      var sdf: SimpleDateFormat  = new SimpleDateFormat("yyyyMMddhhmmss")
      var date: Long = sdf.parse(str.toString).getTime
      date
    })
    //取出观看这个电影的用户，并过滤掉最大时间 - 最小时间 小于6分钟的数据
//    spark.sql("select h.filmName,h.userID from h,e where h.filmName = e.filmName and (data_new(e.zTime) - data_new(h.zTime))/60000 >6")
//      .write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/flie","film_user_name",prop)
  val strs =  spark.sql("select h.filmName,h.userID from h,e where h.filmName = e.filmName and (data_new(e.zTime) - data_new(h.zTime))/60000 >6")
      .map(_.mkString("|")).rdd


    //select zTime,userID from c group by  userID  ORDER BY zTime
//    val conf: Configuration = new Configuration
//    val fs: FileSystem = FileSystem.get(URI.create("hdfs://master01:8020/source/1/a/hello.txt"), conf)
//    val hdfsOutStream: FSDataOutputStream  = fs.create(new Path("hdfs://master01:8020/source/1/a/hello.txt"))
    val ste =strs.collect()

    println("=============1")
    for( i <- 0 until(10)){
      println("=============2")
//      hdfsOutStream.writeChars(ste(i))
      println(ste(0))
      println("=============3")
    }
    println("=============4")


    spark.sqlContext.uncacheTable("c")
    spark.sqlContext.uncacheTable("h")

  }
}

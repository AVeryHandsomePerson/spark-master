package com.cn.carenet.function

import java.io.StringReader
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Date

import org.apache.commons.lang3.time.DateUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.wltea.analyzer.lucene.IKAnalyzer

object TextAnalysis{
  /**
    * TF-IDF是一种简单的文本特征提取算法
    * 词频tf：某个关键词在文本中出现的次数
    * 逆文档频率idf：大小与一个词的常见程度成反比
    * tf=某个词在文章中出现的次数/文章的总词数
    * idf=log(查找的文章总数/(包含该词的文章数+1))
    * tf-idf=tf * idf
    * 未考虑去除停用词（辅助词副词介词等）和语义重构（数据挖掘，数据结构 =》数据，挖掘；数据，结构  50%）
    *
    */
  def ids(): Unit ={
    val spark = SparkSession.builder().master("local[*]").appName("TextAnalysis").getOrCreate()

    val sentenceData = spark.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (0, "I Hi Java could use case classes"),
      (1, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    //建立模型
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    //转换为INT类型的Hashing算法
    //setNumFeatures(100)表示将Hash分桶的数量设置为100个，
    // 这个值默认为2的20次方，即1048576，可以根据你的词语数量来调整，
    // 一般来说，这个值越大，不同的词被计算为一个Hash值的概率就越小，
    // 数据也更准确，但需要消耗更大的内存，和Bloomfilter是一个道理。
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData = hashingTF.transform(wordsData)
    featurizedData.show(false)
    //使用IDF 特征提取算法
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("features", "label").take(3).foreach(println)
//    val model = NaiveBayes.train(trainDataRdd, lambda = 1.0, modelType = "multinomial")
  }
//  def ChineseWordSegmentation(): Unit ={
//    val str = "lxw的大数据田地 -- lxw1234.com 专注Hadoop、Spark、Hive等大数据技术博客。 北京优衣库"
//    val analyzer = new IKAnalyzer(false)
//    val reader = new StringReader(str)
//    val ts = analyzer.tokenStream("", reader)
//    val term = ts.getAttribute(classOf[CharTermAttribute])
//    ts.reset()
//    while(ts.incrementToken()){
//      System.out.print(term.toString()+"|");
//    }
//    analyzer.close
//  }
  def main(args: Array[String]): Unit = {
//  var day = LocalDate.of(2018,4,16)
//  val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
//  var days = day.plusDays(6).format(formatter)
//  var dayss = day.plusDays(7)
//  var daysss =day.plusMonths(1)
//  println(daysss)
val hadoopConf=new Configuration()
  val hdfs= org.apache.hadoop.fs.FileSystem.get(hadoopConf)
  val realPath=new Path("hdfs://master01:8020/source/1/a")
  if(hdfs.exists(realPath)){
    //println("------------")
    hdfs.delete(realPath,true)
    //println()
    //println("------------")
  }
//  val spark = SparkSession.builder().master("local[*]").appName("TextAnalysis").getOrCreate()

//  var sdf: SimpleDateFormat  = new SimpleDateFormat("yyyyMMddhhmmss")
//  var date: Long = sdf.parse("20180626104606").getTime
//  var dates: Long =  sdf.parse("20180626101258").getTime
//  println((date-dates)/60000)



//  val str = spark.sparkContext.textFile("D:\\IDEARFILE\\aaaaa\\file\\2018.06.26.10.evt")
//  str.map(_.split("\\|",-1)).filter(x => {
//    if(x(1).equals("ZTE")) 1  else  true
//  })

  }

}

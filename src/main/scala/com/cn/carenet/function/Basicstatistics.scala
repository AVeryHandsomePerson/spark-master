package com.cn.carenet.function



import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.mllib.linalg.{Matrix, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

object Basicstatistics {
  /**一.基本统计量
    * 统计向量的长度,最大值，最小值，非0个数，模1和模2,方差等
    */
  def statistics(): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("Basicstatistics").getOrCreate()
    val data = spark.sparkContext.parallelize(1 to 100)

    val obs=data.map(x=>Vectors.dense(x))
    val summary=Statistics.colStats(obs)

    println("max is " + summary.max)
    println("min is " + summary.min)
    println("mean is " + summary.mean)
    println("variance is " + summary.variance)
    println("normL1 is " + summary.normL1)
    println("normL2 is " + summary.normL2)

  }

  /**
    * 相关系数
    * 无	−0.09 to 0.0	0.0 to 0.09
    * 弱	−0.3 to −0.1	0.1 to 0.3
    * 中	−0.5 to −0.3	0.3 to 0.5
    * 强	−1.0 to −0.5	0.5 to 1.0
    *
    */
  def Correlationcoefficient(): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("Basicstatistics").getOrCreate()
    val arr3 = (1 to 5).toArray.map(_.toDouble)
    val arr4 = (11 to 15).toArray.map(_.toDouble)
    val rdd7 = spark.sparkContext.parallelize(Array(170.0, 150.0, 210.0,180.0,160.0))
    val rdd8 = spark.sparkContext.parallelize(Array(230.0, 220.0, 210.0,240.0,250.0))
    val rdd3 = spark.sparkContext.parallelize(arr3)
    val rdd4 = spark.sparkContext.parallelize(arr4)
    println(arr3.mkString(","))
    println(arr4.mkString(","))
    val correlation: Double = Statistics.corr(rdd7, rdd8)//计算不同数据之间的相关系数:皮尔逊
    val correlations: Double = Statistics.corr(rdd7, rdd8,"spearman")//使用斯皮尔曼计算不同数据之间的相关系数

    println(correlation)
    println("correlationSpearman：" + correlations) //打印结果
    //    val data=spark.sparkContext.textFile("coo1").map(_.split(",")).map(_.map(_.toDouble)).map(x=>Vectors.dense(x))
    //
    //    val correlMatrix: Matrix = Statistics.corr(data, "pearson")
    //    correlMatrix
  }

  /**
    * 分成抽样
    * RDD有个操作可以直接进行抽样：sampleByKey和sample等
    * 将字符串长度为2划分为层1和层2，对层1和层2按不同的概率进行抽样
    *
    * fractions表示在层1抽0.2，在层2中抽0.8
    * withReplacement false表示不重复抽样
    * 0表示随机的seed
    */
  def DividedIntoSamples(): Unit ={
    val spark = SparkSession.builder().master("local[*]").appName("Basicstatistics").getOrCreate()
    val str = Array("aa","bb","cc","dd","ee","aaa","bbb","ccc","ddd","eee")
    val data = spark.sparkContext.parallelize(str).map(row =>{if(row.length == 3) (row,3) else (row,4)})
      .map(exce =>(exce._2,exce._1))
//   val ma = data.map(_._2).distinct.map(x =>(x.toInt,0.8)).collectAsMap()
    val fractions: Map[Int, Double] = (List((3, 0.2), (4, 0.8))).toMap //设定抽样格式
    val approxSample = data.sampleByKey(withReplacement = false, fractions, 0) //计算抽样样本
    approxSample.foreach(println)
    val randRDD = spark.sparkContext.parallelize(List((7, "cat"), (6, "mouse"), (7, "cup"), (6, "book"), (7, "tv"), (6, "screen"), (7, "heater")))
    val sampleMap = List((7, 0.4), (6, 0.5)).toMap  //设定抽样格式
    val sample2 = randRDD.sampleByKey(false, sampleMap,0).collect//计算抽样样本
    sample2.foreach(println)

    println("Third:")
    //http://bbs.csdn.net/topics/390953396
    val a = spark.sparkContext.parallelize(1 to 20, 3)
    val b = a.sample(true, 0.8, 0)
    val c = a.sample(false, 0.8, 0)
    println("RDD a : " + a.collect().mkString(" , "))
    println("RDD b : " + b.collect().mkString(" , "))
    println("RDD c : " + c.collect().mkString(" , "))
  }

  /**
    * 百分位算子统计
    * percentile函数和percentile_approx函数，其使用方式为percentile(col, p)、percentile_approx(col, p)，p∈(0,1)p∈(0,1)
    * 其中percentile要求输入的字段必须是int类型的，而percentile_approx则是数值类似型的都可以
    * 其实percentile_approx还有一个参数B：percentile_approx(col, p，B)，参数B控制内存消耗的近似精度，
    * B越大，结果的准确度越高。默认为10,000。当col字段中的distinct值的个数小于B时，结果为准确的百分位数。
    * 如果我要求多个分位数怎么办呢？，可以把p换为array(p1,p2,p3…p1,p2,p3…)，即
    * percentile_approx(col,array(0.05,0.5,0.95),9999)
    *如果不放心的话，就给col再加个转换：
    * percentile_approx(cast(col as double),array(0.05,0.5,0.95),9999)
    *没法直接用啊！再加个转换：
    *    explode(percentile_approx(cast(col as double),array(0.05,0.5,0.95),9999))as percentile
    *
    *    实际操作中，发现有时在计算分位数的时候mapper会卡在0%。
    *   前面说过，如果distinct的值小于B，就会返回精确值，
    * 那么个人猜测是因为后台执行的过程是先做了一个select distinct limit B，然后排序得到分位数。
    * 如果distinct值特别多的情况下，仅仅是去重就是一个巨大的运算负担，更别说排序了。而当把B从10000调到100的时候很快就能跑出来了
    */
  def PercentileOperatorStatistics(): Unit ={
    val spark = SparkSession.builder().appName("Basicstatistics").getOrCreate()
    val df =spark.createDataFrame(Seq(
      (0,10,20),
      (1,11,21),
      (2,12,22),
      (3,13,23)
    )).toDF("a","b","")
    df.createOrReplaceTempView("aaa")

    spark.sqlContext.sql("select percentile_approx(cast(a as double),0) as a from aaa").show()
  }

  /**
    * spark RDD 实现 百分位
    * 先对数组进行排序 从小到大
    * @param arr 输入数组
    * @return p 百分位
    *
    */
  def percentile(arr: Array[Double], p: Double) = {
    if (p > 1 || p < 0) throw new IllegalArgumentException("p must be in [0,1]")
    val sorted = Sortingmethod.quickSort(arr)
    val f = (sorted.length + 1) * p
    println("=============>"+f)
    val i = f.toInt
    if (i == 0) sorted.head
    else if (i >= sorted.length) println("=====>1"+sorted.last)
    else {
      val str = (f - i) * (sorted(i) - sorted(i - 1))
      val ste = sorted(i - 1)
     println("=============>2==="+ste.toDouble + str.toDouble)
     println("=============>2==="+(f - i) * (sorted(i) - sorted(i - 1)))
     println("=============>2==="+sorted(i - 1))
    }
  }


  /**
    * LDA
    * 加载了表示文档语料库的字数统计向量。然后，我们使用LDA 从文档中推断出三个主题。
    * 所需簇的数量传递给算法。然后，我们输出主题，表示为词的概率分布。
    */
  def llss(): Unit ={
//    val sparkConf = new SparkConf().setAppName("Basicstatistics").setMaster("local[*]")
//    val sc =new SparkContext(sparkConf)
//    val rdd =sc.textFile("D:\\IDEARFILE\\aaaaa\\file\\sample_lda_data.txt")
//    val vect = rdd.map(s => Vectors.dense(s.trim.split(" ").map(x => x.toDouble)))
    // 此处利用zipWithIndex()将vector与vector在RDD的索引组成键值对，并通过swap()调转索引及词向量 并缓存起来
//    val corpus= vect.zipWithIndex().map(_.swap).cache()
    // 3 建立模型，设置训练参数，训练模型
    /**
      * k: 主题数，或者聚类中心数
      * DocConcentration：文章分布的超参数(Dirichlet分布的参数)，必需>1.0
      * TopicConcentration：主题分布的超参数(Dirichlet分布的参数)，必需>1.0
      * MaxIterations：迭代次数
      * Seed：随机种子
      * CheckpointInterval：迭代计算时检查点的间隔
      * Optimizer：优化计算方法，目前支持"em", "online"
      * *
      * 可在创建lda模型时创建
      * ldaModel = new lda().setK(5).setSeed(5).setDocConcentration(5);
      * *
      * 也可以通过lda模型获取
      *ldaModel.getK();
      *
      */
    // Cluster the documents into three topics using LDA
//    val ldaModel = new LDA().setK(3).run(corpus)
    //    val ldaModel = new LDA().
    //      setK(3).
    //      setDocConcentration(5). //Dirichlet参数，用于事先归档文档在主题上的分布。较大的值可以促进更平滑的推断分布。
    //      setTopicConcentration(5).//关于术语（词）的主题分布之前的Dirichlet参数。较大的值可以促进更平滑的推断分布
    //      setMaxIterations(20).
    //      setSeed(0L).
    //      setCheckpointInterval(10).
    //      setOptimizer("em").run(corpus)
//    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")

    // 返回Matrix 矩阵
//    val topics = ldaModel.topicsMatrix
//
//    for (topic <- Range(0, 3)) {
//      print("Topic " + topic + ":")
//      for (word <- Range(0, ldaModel.vocabSize)) {
//        print(" " + topics(word, topic));// 根据列 下标  和 值下标 去出矩阵数据
//      }
//      println()
//    }
//    ldaModel.save(sc, modelPath)

    val spark =SparkSession.builder().appName("Basicstatistics").master("local[*]").getOrCreate()
    // Loads data.  转换为 libsvm 格式数据
    //加载数据，数据是标签向量。标签可以看作是文档序号。文档格式为:   文档序号  矩阵列序号:文档中的单词
    val dataset = spark.read.format("libsvm").load("D:\\IDEARFILE\\aaaaa\\file\\sample_lda_libsvm_data.txt")
    // Trains a LDA model.
    // 训练lda模型
    val lda = new LDA().setK(10).setMaxIter(10)
    val model = lda.fit(dataset)
    // log likelihood，越大越好。
    val ll = model.logLikelihood(dataset)
    // Perplexity评估，越小越好
    val lp = model.logPerplexity(dataset)
    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    println(s"The upper bound bound on perplexity: $lp")

    val matrix = model.topicsMatrix
    println("------------------------");
    println("矩阵topics列为主题，总共有" + matrix.numCols + "主题");
    println("矩阵topics行为单词，总共有" + matrix.numRows + "单词");
    println("矩阵topics表示的是每个单词在每个主题中的权重");
    for (topic <- Range(0, 3)) {
      print("Topic " + topic + ":")
      for (word <- Range(0, model.vocabSize)) {
        print(" " + matrix(word, topic)); // 根据列 下标  和 值下标 去出矩阵数据
      }
      println()
    }
    // Describe topics.
    // 描述主题只展示概率前三的单词
    val topics = model.describeTopics(3)
    println("The topics described by their top-weighted terms:")
    topics.show(false)
    // Shows the result.
    // 对文档进行聚类，并展示主题分布结果。lable表示的是文档的序号
    val transformed = model.transform(dataset)
    transformed.show(false)
    spark.stop()
  }
  //分段处理
  def Segmentprocessing(): Unit ={
    val spark =SparkSession.builder().appName("Basicstatistics").master("local[*]").getOrCreate()
   val df = spark.read.json("D:\\IDEARFILE\\aaaaa\\file\\people.json")
    //根据条件拆分 0 - 15 范围
    val arr = Array(0,15,Double.PositiveInfinity)
//    val arrs = Array("age","name")

    val bucketDf = new Bucketizer().setInputCol("age").setOutputCol("abc").setSplits(arr).transform(df)

  }




  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder().master("local[*]").appName("Basicstatistics").getOrCreate()
//       df = spark.createDataFrame(data, ["label", "features"])
    PercentileOperatorStatistics
    //    llss()

  }
}

package com.cn.carenet.function


import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{Row, SparkSession}

object Mllib {
//      def aa() ={
//        val conf = new SparkConf().setMaster("local[*]").setAppName(s"Book example: Scala")
//        val sc = new SparkContext(conf)
//
//        // Load 2 types of emails from text files: spam and ham (non-spam).
//        // Each line has text from one email.
//        val spam = sc.textFile("files/spam.txt")
//        val ham = sc.textFile("files/ham.txt")
//
//        // Create a HashingTF instance to map email text to vectors of 100 features.
//        val tf = new HashingTF(numFeatures = 100)
//        // Each email is split into words, and each word is mapped to one feature.
//        val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
//        val hamFeatures = ham.map(email => tf.transform(email.split(" ")))
//
//        // Create LabeledPoint datasets for positive (spam) and negative (ham) examples.
//        val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
//        val negativeExamples = hamFeatures.map(features => LabeledPoint(0, features))
//        val trainingData = positiveExamples ++ negativeExamples
//        trainingData.cache() // Cache data since Logistic Regression is an iterative algorithm.
//
//        // Create a Logistic Regression learner which uses the LBFGS optimizer.
//        val lrLearner = new LogisticRegressionWithSGD()
//        // Run the actual learning algorithm on the training data.
//        val model = lrLearner.run(trainingData)
//
//        // Test on a positive example (spam) and a negative one (ham).
//        // First apply the same HashingTF feature transformation used on the training data.
//        val posTestExample = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))
//        val negTestExample = tf.transform("Hi Dad, I started studying Spark the other ...".split(" "))
//        // Now use the learned model to predict spam/ham for new emails.
//        println(s"Prediction for positive test example: ${model.predict(posTestExample)}")
//        println(s"Prediction for negative test example: ${model.predict(negTestExample)}")
//
//        sc.stop()
//      }
  def Mllib01(): Unit ={
    val spark = SparkSession.builder().master("local[*]").appName("Mllib").getOrCreate()

    // Prepare training data from a list of (label, features) tuples.从（标签，特征）元组列表中准备训练数据。
    val training = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5))
    )).toDF("label", "features")
    // Create a LogisticRegression instance. This instance is an Estimator.
    //创建一个LogisticRegression实例。 这个实例是一个Estimator。
    val lr = new LogisticRegression()
    // Print out the parameters, documentation, and any default values.
    // 打印出参数，文档和任何默认值。
    /**
      * LogisticRegression参数：
      * aggregationDepth：treeAggregate的建议深度（> = 2）（默认值：2）
      * elasticNetParam：ElasticNet混合参数，范围[0，1]。对于α= 0，惩罚是L2惩罚。对于alpha = 1，这是一个L1惩罚（默认值：0.0）
      * family：系列的名称，它是模型中要使用的标签分布的描述。支持的选项：自动，二项式，多项。 （默认：自动）
      * featuresCol：功能列名称（默认：功能）
      * fitIntercept：是否拟合截距项（默认值：true）
      * labelCol：标签列名称（默认：标签）
      * lowerBoundsOnCoefficients：如果在约束约束优化下拟合，系数的下界。 （未定义）
      * lowerBoundsOnIntercepts：如果在约束约束优化下拟合，截距的下限。 （未定义）
      * maxIter：最大迭代次数（> = 0）（默认值：100）
      * predictionCol：预测列名（默认：预测）
      * probabilityCol：预测类别条件概率的列名称。注意：并非所有模型都输出经过良好校准的概率估计！应将这些概率视为置信度，而不是精确概率（默认值：概率）
      * rawPredictionCol：原始预测（a.k.a. confidence）列名（默认值：rawPrediction）
      * regParam：正则化参数（> = 0）（默认值：0.0）
      * 标准化：是否在拟合模型之前对训练特征进行标准化（默认值：true）
      * 阈值：二进制分类预测中的阈值，范围[0，1]（默认值：0.5）
      * 阈值：多类分类中的阈值以调整预测每个类的概率。数组必须具有等于类数的长度，其值> 0，但最多只能有一个值为0.预测具有最大值p / t的类，其中p是该类的原始概率，t是类的数量阈值（未定义）
      * tol：迭代算法的收敛容限（> = 0）（默认值：1.0E-6）
      * upperBoundsOnCoefficients：如果在约束约束优化下拟合，系数的上界。 （未定义）
      * upperBoundsOnIntercepts：如果在约束约束优化下拟合，截距的上限。 （未定义）
      * weightCol：重量栏名称。如果未设置或为空，则我们将所有实例权重视为1.0（未定义）
      */
    //    println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")

    // We may set parameters using setter methods.
    //我们可以使用setter方法来设置参数
    lr.setMaxIter(10).setRegParam(0.01)
    //了解LogisticRegression模型。 这使用存储在lr中的参数。
    val model1 = lr.fit(training)
    // Since model1 is a Model (i.e., a Transformer produced by an Estimator),
    // we can view the parameters it used during fit().
    // This prints the parameter (name: value) pairs, where names are unique IDs for this
    // LogisticRegression instance.
    //由于模型1是模型（即由估计器生成的变形器），
    //我们可以查看fit（）中使用的参数。
    //这将打印参数（名称：值）对，其中名称是唯一的ID
    // LogisticRegression实例。
    println("Model 1 was fit using parameters: " + model1.parent.extractParamMap)

    // We may alternatively specify parameters using a ParamMap, //我们也可以使用ParamMap指定参数，
    // which supports several methods for specifying parameters.  //它支持几种指定参数的方法。
    val paramMap = ParamMap(lr.maxIter -> 20)
      .put(lr.maxIter, 30)  // Specify 1 Param. This overwrites the original maxIter. //指定1参数。 这会覆盖原来的maxIter。
      .put(lr.regParam -> 0.1, lr.threshold -> 0.55)  // Specify multiple Params. 指定多个参数。
    println("paramMap"+paramMap)
    // One can also combine ParamMaps.  也可以结合ParamMaps。
    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")  // Change output column name. 更改输出列名称。
    val paramMapCombined = paramMap ++ paramMap2
    println("paramMap2"+paramMap)
    // Now learn a new model using the paramMapCombined parameters. //现在使用paramMapCombined参数学习一个新模型。
    // paramMapCombined overrides all parameters set earlier via lr.set* methods.  // paramMapCombined覆盖之前通过lr.set *方法设置的所有参数。
    val model2 = lr.fit(training, paramMapCombined)
    println("Model 2 was fit using parameters: " + model2.parent.extractParamMap)

    // Prepare test data.  准备测试数据。
    val test = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
      (0.0, Vectors.dense(3.0, 2.0, -0.1)),
      (1.0, Vectors.dense(0.0, 2.2, -1.5))
    )).toDF("label", "features")

    //使用Transformer.transform（）方法对测试数据进行预测。
    // LogisticRegression.transform只会使用'features'列。
    //注意model2.transform（）输出一个'myProbability'列，而不是通常的
    //'probability'列，因为我们以前重命名了lr.probabilityCol参数。
    model2.transform(test)
      .select("features", "label", "myProbability", "prediction").show()
    //      .collect()
    //      .foreach(println(_))
    //      .foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
    //        println(s"($features, $label) -> prob=$prob, prediction=$prediction")
    //      }
  }

  def main(args: Array[String]) {

    val spark = SparkSession.builder().master("local[*]").appName("Mllib").getOrCreate()
    // Prepare training documents from a list of (id, text, label) tuples.
    //从（id，text，label）元组列表中准备训练文档。
    val training = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text", "label")

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    //配置一个ML管道，它由三个阶段组成：tokenizer，hashingTF和lr。
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

    // Fit the pipeline to training documents.
    //将管道安装到培训文档。
    val model = pipeline.fit(training)

    // Now we can optionally save the fitted pipeline to disk
    //现在我们可以选择将拟合管道保存到磁盘
    model.write.overwrite().save("/spark-logistic-regression-model")

    // We can also save this unfit pipeline to disk
    //我们也可以将这个不适当的管道保存到磁盘
    pipeline.write.overwrite().save("/unfit-lr-model")

    // And load it back in during production
    //在生产过程中重新加载
    val sameModel = PipelineModel.load("/spark-logistic-regression-model")

    // Prepare test documents, which are unlabeled (id, text) tuples.
    //准备测试文档，它们是标记的（id，text）元组。
    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "spark hadoop spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    // Make predictions on test documents. 对测试文档进行预测。
    model.transform(test)
      .select("id", "text", "probability", "prediction").show()
//      .collect()
//      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
//        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
//      }
  }

}

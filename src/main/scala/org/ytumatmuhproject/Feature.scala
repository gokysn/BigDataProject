package org.ytumatmuhproject

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.{ClusteringEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.functions.{col, desc, when}


object Feature {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Spark Session oluşturma
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("OutlierDetection")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "2g")
      .getOrCreate()


    val sc = spark.sparkContext

    val kMeansDF = spark.read.format("csv")
      .option("sep", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:/Users/gokys/Desktop/airbnb/nonNull.csv")

    // kategorik değişkeni StringIndexer ile nümerik yapalım

    //Vector Assembler ile tüm girdileri bir vektör haline getirelim
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("accommodates" ,"guests_included", "bathrooms", "beds", "bedrooms"))
      .setOutputCol("vectorizedFeatures")

    // Regresyon Modeli oluşturma
    import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
    val linearRegressionObject = new LinearRegression()
      .setLabelCol("guests_included")
      .setFeaturesCol("vectorizedFeatures")


    // Pipeline oluşturma
    import org.apache.spark.ml.Pipeline
    val pipelineObject = new Pipeline()
      .setStages(Array(vectorAssembler, linearRegressionObject))

    // Veri setini train ve test olarak ayırma
    val Array(trainDF, testDF) = kMeansDF.randomSplit(Array(0.75, 0.25),142L)

    // Modeli eğitme
    val pipelineModel = pipelineObject.fit(trainDF)

    // Artıkları kendimiz hesaplayıp tahmin, gerçek değer ile yan yana inceleyelim
    val resultDF = pipelineModel.transform(testDF)
    resultDF.withColumn("residuals", (resultDF.col("guests_included") - resultDF.col("prediction"))).show()

    // Pipeline model içinden lrModeli alma
    val lrModel = pipelineModel.stages.last.asInstanceOf[LinearRegressionModel]

    val evaluator1 = new MulticlassClassificationEvaluator()
      .setLabelCol("guests_included")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator1.evaluate(resultDF)

    println(accuracy)


    // Regresyon modele ait  istatistikler
    println(s"RMSE: ${lrModel.summary.rootMeanSquaredError}")
    println(s"R kare : ${lrModel.summary.r2}")
    println(s"Düzeltilmiş R kare : ${lrModel.summary.r2adj}")
    // Değişken katsayılarını görme. Son değer sabit
    println(s"Katsayılar : ${lrModel.coefficients}")
    println(s"Sabit : ${lrModel.intercept}")
    // p değerlerini görme. Not: Son değer sabit için
    println(s"p değerler: [${lrModel.summary.pValues.mkString(",")}]")
    // t değerlerini görme. Not: Son değer sabit için
    println(s"t değerler: [${lrModel.summary.tValues.mkString(",")}]")
    // Regresyon denklem.: y =

    // Dataframe içinde tahmin edilen değerlerle gerçekleri görelim
    lrModel.summary.predictions.show()
    lrModel.summary.predictions.groupBy("prediction").count().show(false)





}
}

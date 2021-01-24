package org.ytumatmuhproject

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}


object isBusinessTravelReady {

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

    val newKmeansDF = kMeansDF.withColumn("ozel_çalisma_alani",
      when(col("amenities").contains("workspace"),"iş seyahati için uygun")
        .otherwise("iş seyahati için uygun değil")
    )
    println("ozel çalisma alani DF:")
    newKmeansDF.show(false)

    
    // kategorik değişkeni StringIndexer ile nümerik yapalım
     val stringIndexer = new StringIndexer()
       .setHandleInvalid("skip")
       .setInputCol("property_type")
       .setOutputCol("property_typeIndex")


    val stringIndexerModel = stringIndexer.fit(newKmeansDF)
    val stringIndexedDF = stringIndexerModel.transform(newKmeansDF)

    println("room_type sütunu için StringIndex eklenmiş DF: room_typeIndexedDF")
    stringIndexedDF.show()

    val roomTypeStringIndexer = new StringIndexer()
      .setHandleInvalid("skip")
      .setInputCol("room_type")
      .setOutputCol("room_typeIndex")

    val roomTypeStringIndexerModel = roomTypeStringIndexer.fit(stringIndexedDF)
    val roomTypeStringIndexedDF = roomTypeStringIndexerModel.transform(stringIndexedDF)

     // kategorik değişkeni one hot encoder
     val oneHotEncoder = new OneHotEncoderEstimator()
       .setInputCols(Array("property_typeIndex" ,"room_typeIndex"))
       .setOutputCols(Array("property_typeEncoded", "room_typeEncoded"))


    val encoderModel = oneHotEncoder.fit(roomTypeStringIndexedDF)
    val oneHotEncodeDF = encoderModel.transform(roomTypeStringIndexedDF)


    println("oneHotEncodeDF:")
    oneHotEncodeDF.show()

    //Vector Assembler ile tüm girdileri bir vektör haline getirelim
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("property_typeEncoded", "room_typeEncoded"))
      .setOutputCol("vectorizedFeatures")

    val vectorAssembledDF = vectorAssembler.transform(oneHotEncodeDF)

    println("vectorAssembledDF: ")
    vectorAssembledDF.show(false)


    val labelIndexer = new StringIndexer()
      .setInputCol("ozel_çalisma_alani")
      .setOutputCol("label")

    val labelIndexerModel = labelIndexer.fit(vectorAssembledDF)
    val labelIndexerDF = labelIndexerModel.transform(vectorAssembledDF)

    println("labelIndexerDF : ")
    labelIndexerDF.show(truncate = false)

    // StandardScaler
    val scaler = new StandardScaler()
      .setInputCol("vectorizedFeatures")
      .setOutputCol("features")


    val scalerModel = scaler.fit(labelIndexerDF)
    val scalerDF = scalerModel.transform(labelIndexerDF)

    println("scalerDF: ")
    scalerDF.show(truncate = false)

    val Array(trainDF, testDF) =  scalerDF.randomSplit(Array(0.8, 0.2), 142L)

    println("trainDF: ")
    trainDF.show(false)
    println("testDF: ")
    testDF.show(false)

    ////////////////// Basit Bir Makine Öğrenmesi Modeli//////////////////////////////////
    // ====================================================================================

    // Sınıflandırma-lojistik regresyon olsun
    import org.apache.spark.ml.classification._

    val lrClassifier = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setPredictionCol("prediction")

    val lrModel = lrClassifier.fit(trainDF)

   // println("train dataset")
    //lrModel.transform(trainDF).show(false)


    println("test dataset")
    lrModel.transform(testDF).show(false)
    val results = lrModel.transform(testDF).select("label", "prediction")
    results.groupBy("label").count().show(false)
    results.groupBy("prediction").count().show(false)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(results)

    println(accuracy)

  }

}


package org.ytumatmuhproject

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions.{asc, count, desc}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StringType

object KMeans {
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

    val Array(trainDF, testDF) = kMeansDF.randomSplit(Array(0.7,0.3), 142L)
    println("trainDF count: "+ trainDF.count())
    println("testDF count: "+ testDF.count())



    ////////////////////  FONKSİYONLAR  /////////////////////////////
    // Kategorik ve nümerik nitelikleri birbirinden ayırma
    def identifyCategoricAndNumeric(kMeansDF:DataFrame,colsToAnalyze: Array[String]): (Array[String], Array[String]) = {
      /*
      Bu fonksiyon parametre olarak dataframe(kMeansDF) ve analize girecek sütun isimlerini (colsToAnalyze) alır ve sonuç olarak
      kategorik ve nümerik niteliklerin bulunduğu 2 array döndürür.
       */
      var categoricCols = Array[String]()
      var numericCols = Array[String]()
      for (col <- colsToAnalyze) {
        if (kMeansDF.schema(col).dataType == StringType) {
          categoricCols = categoricCols ++ Array(col)
        } else {
          numericCols = numericCols ++ Array(col)
        }
      }

      (categoricCols, numericCols)
    }
    val notToAnalyzed = Array("experiences_offered",
      "has_availability",
      "is_business_travel_ready",
      "calendar_last_scraped",
      "last_scraped",
      "host_since",
      "requires_license")


    // Analize girmeyecek nitelikleri filtreleme ve sadece gireceklerden bir Array oluşturma
    val colsToAnalyze = kMeansDF.columns.toSet.diff(notToAnalyzed.toSet).toArray.sorted

    // kategorik değişkenlerle nümerikleri ayıran fonksiyonu kullanarak ayrı ayrı iki Array'e atama
    val (categoricalCols, numericalCols) = identifyCategoricAndNumeric(kMeansDF,colsToAnalyze)

    // Ayırma sonucunu görelim
    println("Kategorik nitelikler:")
    categoricalCols.foreach(println(_))
    println("\nNümerik nitelikler:")
    numericalCols.foreach(println(_))

    kMeansDF.describe(numericalCols:_*).show()


    // StringIndexer, OneHotEncoder ve VectorAssembler için kategorik ön hazırlık
    // Kategorik değişkenler için StringIndexer ve OneHotEncoderları toplayacak boş Array
    var categoricalColsPipeStages = Array[PipelineStage]()

    // Kategorik değişkenler için OneHotEncoder çıktı isimlerini toplayıp VectorAssembler'a verecek boş Array
    var catsForVectorAssembler = Array[String]()

    // StringIndexer, OneHotEncoder ve VectorAssembler için for loop yap. Tüm kategorik değişkenleri
    // sırasıyla StringIndexer, OneHotEncoder'dan geçir ve VectorAssembler'a hazır hale getir.
    // Yani Vector Assembler'a hangi sütun isimlerini verecek isek onları bir Array'de topla
    for (col <- categoricalCols) {

      val stringIndexer = new StringIndexer() // StringIndexer nesnesi oluştur
        .setHandleInvalid("skip") // kayıp kategorileri ihmal et
        .setInputCol(col) // girdi sütun ne
        .setOutputCol(s"${col}Index") // çıktı sütün ismi sütun ismi Index eklenerek oluşsun

      val stringIndexerModel = stringIndexer.fit(kMeansDF)
      val stringIndexedDF = stringIndexerModel.transform(kMeansDF)

      println("Tüm sütunlar için StringIndex eklenmiş DF")
      stringIndexedDF.show(false)

      println(s"\n$col için groupby")
      stringIndexedDF.groupBy(col).count().sort(desc("count")).show(false)



      //stringIndexedDF.groupBy("amenitiesIndex").count().orderBy(asc("count")).show(false)


      val encoder = new OneHotEncoderEstimator() // nesne oluştur
        .setInputCols(Array(s"${col}Index")) // stringIndexer çıktı sütunu burada girdi oluyor
        .setOutputCols(Array(s"${col}OneHot")) // çıktı sütun ismi OneHot eklenerek oluşsun

      // Her döngüde pipeline stage olarak string indexer ve encoder ekle
      categoricalColsPipeStages = categoricalColsPipeStages :+ stringIndexer :+ encoder

      // Vector Assmebler da kullanılmak üzere her döngüde OneHotEncoder çıktısı sütun isimlerini listeye ekle
      catsForVectorAssembler = catsForVectorAssembler :+ s"${col}OneHot"

      println(catsForVectorAssembler.mkString("Array(", ", ", ")"))



      /*val encoderModel = encoder.fit(stringIndexedDF)
      val oneHotEncodeDF = encoderModel.transform(stringIndexedDF)

      println("oneHotEncodeDF:")
      oneHotEncodeDF.show(false)*/

    }
      // VectorAssembler
      val vectorAssembler = new VectorAssembler() // nesne oluştur
        .setInputCols(numericalCols ++ catsForVectorAssembler) // nümerik nitelikler ile içinde onehotencoder çıktı nitelikleri olan kategorik nitelikler
        .setOutputCol("features") // çıktı sütun ismi features olsun

    /*  val vectorAssembledDF = vectorAssembler.transform(oneHotEncodeDF)

      println("vectorAssembledDF: ")
      vectorAssembledDF.show(false)*/

      ////////////////// LabelIndexer AŞAMASI ////////////////////////////////////////////
      // ====================================================================================
      /*val labelIndexer = new StringIndexer()
        .setInputCol("bed_type")
        .setOutputCol("label")

      val labelIndexerModel = labelIndexer.fit(vectorAssembledDF)
      val labelIndexerDF = labelIndexerModel.transform(vectorAssembledDF)

      println("labelIndexerDF: ")
      labelIndexerDF.show(truncate = false)

      labelIndexerDF.groupBy("label").count().orderBy(desc("count")).show(50, false)*/


      val standartScaler = new StandardScaler() // nesne yarat
        .setInputCol("features") // girdi olarak vector assembler ın bir sütuna vektör olarak birleştirdiği niteliği al
        .setOutputCol("scaledFeatureVector") // çıktı ismini belirle
        .setWithStd(true) // hiperparametre
        .setWithMean(false) // hiperparametre

      /*val scalerModel = standartScaler.fit(labelIndexerDF)
      val scalerDF = scalerModel.transform(labelIndexerDF)

      println("scalerDF: ")
      scalerDF.show(truncate = false)*/

    /*val classifier = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")

    val pipelineObj = new Pipeline()
      .setStages(categoricalColsPipeStages ++ Array(vectorAssembler, standartScaler, classifier))

    val pipelineModel = pipelineObj.fit(trainDF)

    pipelineModel.transform(testDF).select("label","prediction").show()*/


      // KMeans
      val kmeansObject = new KMeans() // nesne
        .setSeed(142) // random seed değeri başa bir değer de olabilir
        //.setK(5) // kullanıcık değerini belirlemesi gerek. Bunu yapacağız.
        .setPredictionCol("cluster") // sonuçlar (küme numaralarının bulunacağı yeni sütun adı ne olsun)
        .setFeaturesCol("scaledFeatureVector") // girdi olarak StandartScaler çıktısını alıyoruz
        .setMaxIter(40) // Hiperparametre: iterasyon sayısı
        .setTol(1.0e-5) // Hiperparametre

      /*val kMeansModel = kmeansObject.fit(scalerDF)
      val kMeansDataframe = kMeansModel.transform(scalerDF)

      println("kMeansDataFrame : ")
      kMeansDataframe.show(truncate = false)

      kMeansDataframe.groupBy("cluster").count().show(false)*/


      // pipeline model: parçaları birleştir
      val pipelineObject = new Pipeline()
        .setStages(categoricalColsPipeStages ++ Array(vectorAssembler, standartScaler, kmeansObject))

      // pipeline ile modeli eğitme ve pipeline model elde etme
      val pipelineModel = pipelineObject.fit(trainDF) //kMeansDataframe

      // Veri setini eğitilmiş modelden geçirelim ve tahminleri görelim: Kim hangi kümeye düşmüş
      val transformedDF = pipelineModel.transform(testDF)

      transformedDF.show()

    transformedDF.groupBy("cluster").count().show(false)

    val evaluator = new MulticlassClassificationEvaluator()
      .setPredictionCol("cluster")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(transformedDF)

    println(accuracy)

  }

}

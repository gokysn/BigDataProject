package org.ytumatmuhproject

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{MinMaxScaler, OneHotEncoderEstimator, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc



object Normalization {

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

    kMeansDF.describe().show(false)

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("beds"))
      .setOutputCol("vectorizedFeatures")

    val vectorAssembledDF = vectorAssembler.transform(kMeansDF)


    println("vectorAssembledDF: ")
    vectorAssembledDF.show(false)

    vectorAssembledDF.groupBy("vectorizedFeatures").count().show(false)
    println(vectorAssembledDF.groupBy("vectorizedFeatures").count())





    val scaler = new MinMaxScaler()
      .setInputCol("beds")
      .setOutputCol("features").setMax(1).setMin(0)

    val scalerModel = scaler.fit(vectorAssembledDF)
    val scalerDF = scalerModel.transform(vectorAssembledDF)

    println("scalerDF: ")
    scalerDF.show(truncate = false)

    scalerDF.groupBy("features").count().orderBy(desc("count")).show(false)


  }
}

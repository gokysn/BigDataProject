package org.ytumatmuhproject

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col}

object DataCleaning {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Spark Session oluşturma
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("RDD-Olusturmak")
      .config("spark.executor.memory","4g")
      .config("spark.driver.memory","2g")
      .getOrCreate()


    val sc = spark.sparkContext

    //Dosyadan Veri Okumak
    val dfFromFile = spark.read.format("csv")
      .option("sep",",")
      .option("header","true")
      .option("inferSchema","true")
      .load("C:/Users/gokys/Desktop/airbnb/total_data.csv")

   val df2 =  dfFromFile
      .drop("_c0","scrape_id","last_scraped","thumbnail_url","medium_url","xl_picture_url","neighbourhood_group_cleansed","license","jurisdiction-names")
      .filter(col("experiences_offered") === "none")

    df2.na.drop()
       .show()

  }
}

package org.ytumatmuhproject

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession}
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
      .option("mode","DROPMALFORMED")
      .load("C:/Users/gokys/Desktop/airbnb/total_data.csv")


    // okunan dataframe'e ilk bakış
    println("\n Orijinal DF")
    dfFromFile.show(20)


   val df2 = dfFromFile
     .drop("_c0","description","minimum_maximum_nights","maximum_minimum_nights","minimum_minimum_nights","maximum_maximum_nights","minimum_nights_avg_ntm","maximum_nights_avg_ntm","number_of_reviews_ltm","calculated_host_listings_count_entire_homes","calculated_host_listings_count_private_rooms","calculated_host_listings_count_shared_rooms","space","summary","name","scrape_id","last_scraped","thumbnail_url","medium_url","xl_picture_url","neighbourhood_group_cleansed","license","jurisdiction-names")
     .filter(col("experiences_offered") === "none")
     .orderBy("id")

    df2
      .select("id", "amenities")
      .filter(col("experiences_offered") === "none")
      .groupBy("id","amenities")
      .count()
      .orderBy("id")
      .show(false)


  }
}

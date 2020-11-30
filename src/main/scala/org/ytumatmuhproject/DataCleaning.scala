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
    import spark.implicits._

    //Dosyadan Veri Okumak
    val dfFromFile = spark.read.format("csv")
      .option("sep",",")
      .option("header","true")
      .option("inferSchema","true")
      .load("C:/Users/gokys/Desktop/airbnb/total_data.csv")


    // okunan dataframe'e ilk bakış

    println("\n Orijinal DF")
    dfFromFile.show(20)

    println(dfFromFile.count())
    


    // Tüm değerleri null olan fieldları  unique olan experiences_offered fieldına göre drop ediyoruz.

   val filterdf = dfFromFile
     .drop("_c0","description","minimum_maximum_nights","neighborhood_overview","transit","access","interaction","house_rules","host_acceptance_rate","notes","maximum_minimum_nights","minimum_minimum_nights","maximum_maximum_nights","minimum_nights_avg_ntm","maximum_nights_avg_ntm","number_of_reviews_ltm","calculated_host_listings_count_entire_homes","calculated_host_listings_count_private_rooms","calculated_host_listings_count_shared_rooms","space","summary","name","thumbnail_url","medium_url","xl_picture_url","neighbourhood_group_cleansed","license","jurisdiction-names")
     .filter(col("experiences_offered") === "none")
     .orderBy(("id"))

   println(filterdf.count())


   val urlFilterdf = filterdf
      .filter(col("listing_url").contains("https") || col("host_url").contains("https") || col("host_thumbnail_url").contains("https") || col("host_picture_url").contains("https") || col("host_is_superhost").contains("t") || col("host_is_superhost").contains("f") || col("host_has_profile_pic").contains("t") || col("host_has_profile_pic").contains("f") || col("host_identity_verified").contains("t") || col("host_identity_verified").contains("f"))
      .orderBy("host_id")
    println(urlFilterdf.count())


    val hasHostFilterdf = urlFilterdf
      .filter(col("host_is_superhost").contains("t") || col("host_is_superhost").contains("f") || col("host_has_profile_pic").contains("t") || col("host_has_profile_pic").contains("f") || col("host_identity_verified").contains("t") || col("host_identity_verified").contains("f"))
      .orderBy("host_id")

    println(hasHostFilterdf.count())

    val countFilterdf = hasHostFilterdf
      .filter(col("host_listings_count").contains(".0") || col("host_total_listings_count").contains(".0") )
     .orderBy("id")

    println(countFilterdf.count())

    val streetFilterdf = countFilterdf
      .filter(col("street").contains("Brazil") )
      .orderBy("id")

    println(streetFilterdf.count())


    val locationFilterdf  = streetFilterdf
      .filter(col("smart_location").contains("Brazil") || col("country_code").contains("BR") || col("country").contains("Brazil"))
      .orderBy("id")

    locationFilterdf.distinct()

    println(locationFilterdf.count())


      val roomFilterdf  = locationFilterdf
      .filter(col("bedrooms").contains(".0") || col("bathrooms").contains(".0") || col("beds").contains(".0"))
      .orderBy("id")

    println(roomFilterdf.count())

    val amenitiesdf  = roomFilterdf
      .filter(!col("amenities").contains("{}") )
      .orderBy("id")

    println(amenitiesdf.count())


    val amenities2df = amenitiesdf
      .filter(col("amenities").contains("}"))
      .orderBy("id")

    println(amenities2df.count())

    amenities2df.show(100,false)




    // Her bir sütun için for dongüsü ile o sütunda filter() metodu ile null kontrolü yapalım ve bunları sayalım. Eğer
    // O sütunda en az bir adet null varsa yani >0 ise o sütunda null olduğunu ekrana yazdıralım.
    /* val sutunlar = priceFilter.columns
    println("\n ////////////// Null Kontrolü ///////////////////// \n")
    var sayacForNull = 1
    for (sutun <- sutunlar) {
      if (priceFilter.filter(col(sutun).isNull).count() > 0) {
        println(sayacForNull + ". " + sutun + " içinde null değer var.")
      } else {
        println(sayacForNull + ". " + sutun)
      }
      sayacForNull += 1
    }*/


  }
}

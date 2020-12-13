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
      .option("quote", "\"")
      .option("escape", "\"")
      .option("ignoreLeadingWhiteSpace", true)
      .option("header","true")
      .option("inferSchema","true")
      .load("C:/Users/gokys/Desktop/airbnb/total_data.csv")


    // okunan dataframe'e ilk bakış

    println("\n Orijinal DF")
    dfFromFile.show(20)

    println(dfFromFile.count())
    


    // Tüm değerleri null olan fieldları  unique olan experiences_offered fieldına göre drop ediyoruz.

   val prefilters = dfFromFile
     .drop("_c0","host_acceptance_rate","thumbnail_url","medium_url","xl_picture_url","neighbourhood_group_cleansed","license")
     .filter(col("experiences_offered") === "none")
     .orderBy(("id"))

    println(prefilters.count())

    prefilters.show(false)




    /*val nonNullDF = amenities2df.na.fill("--")

    nonNullDF.show(false)

   prefilters
      .repartition(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .save("C:/Users/gokys/Desktop/cleanData")


    // Her bir sütun için for dongüsü ile o sütunda filter() metodu ile null kontrolü yapalım ve bunları sayalım. Eğer
    // O sütunda en az bir adet null varsa yani >0 ise o sütunda null olduğunu ekrana yazdıralım.
     val sutunlar = roomFilterdf.columns
    println("\n ////////////// Null Kontrolü ///////////////////// \n")
    var sayacForNull = 1
    for (sutun <- sutunlar) {
      if (roomFilterdf.filter(col(sutun).isNull).count() > 0) {
        println(sayacForNull + ". " + sutun + " içinde null değer var.")
      } else {
        println(sayacForNull + ". " + sutun)
      }
      sayacForNull += 1
    }*/

  }
}

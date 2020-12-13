package org.ytumatmuhproject

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,desc}

object Preprocess {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Spark Session oluşturma
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("Preprocess")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "2g")
      .getOrCreate()


   val sc = spark.sparkContext
    import spark.implicits._

    val cleandatadf = spark.read.format("csv")
      .option("sep",",")
      .option("header","true")
      .option("inferSchema","true")
      .load("C:/Users/gokys/Desktop/airbnb/cleandataset.csv")

   // drop.printSchema()

    val dropfilter = cleandatadf
      .drop("summary",
        "space",
        "neighborhood_overview",
        "notes",
        "transit",
        "access",
        "interaction",
        "house_rules",
        "host_about",
        "host_response_time",
        "host_response_rate",
        "description",
        "name",
        "host_neighbourhood",
        "neighbourhood",
        "zipcode",
        "square_feet",
        "weekly_price",
        "monthly_price",
        "security_deposit",
        "cleaning_fee",
        "first_review",
        "last_review",
        "review_scores_rating",
        "review_scores_accuracy",
        "review_scores_cleanliness",
        "review_scores_checkin",
        "review_scores_communication",
        "review_scores_location",
        "review_scores_value",
        "jurisdiction_names",
        "reviews_per_month",
        "minimum_minimum_nights",
        "maximum_minimum_nights",
        "minimum_maximum_nights",
        "maximum_maximum_nights",
        "minimum_nights_avg_ntm",
        "maximum_nights_avg_ntm",
        "number_of_reviews_ltm",
        "calculated_host_listings_count_entire_homes",
        "calculated_host_listings_count_private_rooms",
        "calculated_host_listings_count_shared_rooms")
    val drop = dropfilter
      .na.drop(Array("host_location","host_is_superhost","host_total_listings_count","host_listings_count","host_has_profile_pic","host_identity_verified","city","state","market","bathrooms","bedrooms","beds"))
      .na.fill(3.0,Array("beds"))
      .na.fill(2.0,Array("bedrooms"))
      .na.fill(2.0,Array("bathrooms"))
      .na.fill("Rio De Janeiro" ,Array("market"))
      .na.fill("Rio de Janeiro" ,Array("city"))
      .na.fill("Rio de Janeiro" ,Array("state"))
      .na.fill("f",Array("host_identity_verified","host_is_superhost"))
      .na.fill("t",Array("host_has_profile_pic"))
      .na.fill(8.75,Array("host_total_listings_count"))
      .na.fill(8.75,Array("host_listings_count"))
      .na.fill("Rio de Janeiro, State of Rio de Janeiro, Brazil",Array("host_location"))

    println(drop.count())
    drop.show(false)

    /*val sutunlar = drop.columns
    println("\n ////////////// Null Kontrolü ///////////////////// \n")
    var sayacForNull = 1
    for (sutun <- sutunlar) {
      if (drop.filter(col(sutun).isNull).count() > 0) {
        println(sayacForNull + ". " + sutun + " içinde null değer var.")
      } else {
        println(sayacForNull + ". " + sutun)
      }
      sayacForNull += 1
    }*/

   /* drop
      .repartition(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .save("C:/Users/gokys/Desktop/NonNulldataset")*/

   /* drop.select("host_listings_count")
      .groupBy("host_listings_count")
      .count()
      .orderBy(desc("count"))
      .show(100,false)

    drop.select("city")
      .groupBy("city")
      .count()
      .orderBy(desc("count"))
      .show(100,false)
    drop.select("state")
      .groupBy("state")
      .count()
      .orderBy(desc("count"))
      .show(100,false)

    drop.select("market")
      .groupBy("market")
      .count()
      .orderBy(desc("count"))
      .show(100,false)
    drop.select("bathrooms")
      .groupBy("bathrooms")
      .count()
      .orderBy(desc("count"))
      .show(100,false)
    drop.select("bedrooms")
      .groupBy("bedrooms")
      .count()
      .orderBy(desc("count"))
      .show(100,false)

    drop.select("beds")
      .groupBy("beds")
      .count()
      .orderBy(desc("count"))
      .show(100,false)*/

  }
}

package org.ytumatmuhproject

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc}

object DataExplore {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Spark Session oluşturma
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("RDD-Olusturmak")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "2g")
      .getOrCreate()


    //Dosyadan Veri Okumak
    val df2 = spark.read.format("csv")
      .option("sep", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:/Users/gokys/Desktop/airbnb/cleandataset.csv")


    // okunan dataframe'e ilk bakış
    println("\n Orijinal DF")
    df2.show(20)

   val filter = df2
     .drop("_c0","host_acceptance_rate","thumbnail_url","medium_url","xl_picture_url","neighbourhood_group_cleansed","license","jurisdiction-names")
     .filter(col("experiences_offered") === "none")
     .orderBy(("id"))

   println(filter.count())

    filter.groupBy("id")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

// null ve string değerler var

    filter.groupBy("listing_url")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

// https içeren değerler gelmeli gerisi uçacak.
    filter.groupBy("experiences_offered")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    filter.groupBy("host_id")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    filter.groupBy("host_url")
      .count()
      .orderBy(desc("count"))
      .show(50,false)
//https içermeli
    filter.groupBy("host_name")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    filter.groupBy("host_since")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    filter.groupBy("host_location")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    filter.groupBy("host_response_time")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    filter.groupBy("host_response_rate")
      .count()
      .orderBy(desc("count"))
      .show(50,false)
// % içermeyenleri uçur.

    filter.groupBy("host_is_superhost")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    // f ve t içermeyenleri uçur.

    filter.groupBy("host_listings_count")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    // 32.0 içermeyenleri uçur.

    filter.groupBy("host_verifications")
      .count()
      .orderBy(desc("count"))
      .show(50,false)
    // [] içermeyenleri uçur.

    filter.groupBy("host_total_listings_count")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    // .0 içermeyenleri uçur.

    filter.groupBy("host_has_profile_pic")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    // t ve f içermeyenleri uçur.

    filter.groupBy("host_identity_verified")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    // t ve f içermeyenleri uçur.

    filter.groupBy("street")
      .count()
      .orderBy(desc("count"))
      .show(50,false)
    // Brazil içerenleri filtrele.

    filter.groupBy("neighbourhood")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    // t, f, 1.0 ,[] içermemesi lazım.
    filter.groupBy("neighbourhood_cleansed")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    // t, f, 1.0 ,[] içermemesi lazım.

    filter.groupBy("city")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    //t ve f içermemesi lazım.
    filter.groupBy("state")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    // Rio de janeiro ve Rj içermeli

    filter.groupBy("zipcode")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    // Rio de janeiro olmayacak ve null

    filter.groupBy("market")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    //Rio De jenario Rio de jenario içermeli sadece

    filter.groupBy("smart_location")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    // Brazil içersin yeterli

    filter.groupBy("country_code")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    //sadece BR içermeli

    filter.groupBy("country")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    // Brazil içermeli sadece

    filter.groupBy("latitude")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    //- işareti içermeli -22 ve -23 olabilir.

    filter.groupBy("longitude")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    // - içermeli ve -43 42 44

    filter.groupBy("is_location_exact")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    // t,f içermeli gerisi uçacak

    filter.groupBy("property_type")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    // Apartment house condomium Loft bed and breakfeast guest suite hostel villa  içermeli

    filter.groupBy("room_type")
      .count()
      .orderBy(desc("count"))
      .show(50,false)
    // entire home private room |Hotel room Shared room içermeli

    filter.groupBy("accommodates")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    // Sayıları içermeli gerisini uçur.

    filter.groupBy("bathrooms")
      .count()
      .orderBy(desc("count"))
      .show(50,false)
    // . içermeli 4.0 gibi

    filter.groupBy("bedrooms")
      .count()
      .orderBy(desc("count"))
      .show(50,false)
      //. içermeli 4.0 gibi

    filter.groupBy("beds")
      .count()
      .orderBy(desc("count"))
      .show(50,false)
    // . içermeli 4.0 gibi

    filter.groupBy("bed_type")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    // Real bed and pull_out sofa içermeli gerisi uçacak

    filter.groupBy("amenities")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    // " işaretini içermeli gerisi uçacak

    filter.groupBy("square_feet")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    //.0 içermeli gerisi uçacak

    filter.groupBy("price")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    // $ ve .00 içermeli gerisi uçacak..

    filter.groupBy("weekly_price")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    //$ ve, .00 içermeli komple de uçabilir.

    filter.groupBy("monthly_price")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    //$ ve, .00 içermeli komple de uçabilir.

    filter.groupBy("security_deposit")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    //$ ve, .00 içermeli.

    filter.groupBy("cleaning_fee")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    //$ ve, .00 içermeli

    filter.groupBy("guests_included")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    // Sayıları içermeli 1,2,4,10 gibii

    filter.groupBy("extra_people")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    //$, .00 içermeli

    filter.groupBy("minimum_nights")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    //Sayıları içermeli 1,2,4,10 gibii

    filter.groupBy("maximum_nights")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    // Sayıları içermeli 30,1125 gibi

    filter.groupBy("calendar_updated")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    // today, moonts ,week ago gibi keliemleri içermeli

    filter.groupBy("has_availability")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    //t,f içermeli sadece

    filter.groupBy("availability_30")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    //Sayılardan 0-30 arasını içermeli.

    filter.groupBy("availability_60")
      .count()
      .orderBy(desc("count"))
      .show(50,false)
    //Sayılardan 0-60 arasını içermeli.

    filter.groupBy("availability_90")
      .count()
      .orderBy(desc("count"))
      .show(50,false)
    //Sayılardan 0-90 arasını içermeli.

    filter.groupBy("availability_365")
      .count()
      .orderBy(desc("count"))
      .show(50,false)
    //Sayılardan 0-365 arasını içermeli.

    filter.groupBy("calendar_last_scraped")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    //2018-08-16 tarihi içermeli

    filter.groupBy("number_of_reviews")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    // Sayıları içermeli

    filter.groupBy("first_review")
      .count()
      .orderBy(desc("count"))
      .show(50,false)
    //2018-08-16 tarihi içermeli

    filter.groupBy("last_review")
      .count()
      .orderBy(desc("count"))
      .show(50,false)
    //2018-08-16 tarihi içermeli

    filter.groupBy("review_scores_rating")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    //.0 içermeli

    filter.groupBy("review_scores_accuracy")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

      //.0 içermeli


    filter.groupBy("review_scores_cleanliness")
      .count()
      .orderBy(desc("count"))
      .show(50,false)
    //.0 içermeli

    filter.groupBy("review_scores_checkin")
      .count()
      .orderBy(desc("count"))
      .show(50,false)
    //.0 içermeli


    filter.groupBy("review_scores_communication")
      .count()
      .orderBy(desc("count"))
      .show(50,false)
    //.0 içermeli
    filter.groupBy("review_scores_location")
      .count()
      .orderBy(desc("count"))
      //.0 içermeli
      .show(50,false)

    filter.groupBy("review_scores_value")
      .count()
      .orderBy(desc("count"))
      .show(50,false)
    //.0 içermeli

    filter.groupBy("requires_license")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    // t,f içermeli

    filter.groupBy("instant_bookable")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    //t,f içermeli

    filter.groupBy("is_business_travel_ready")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    //t,f içermeli

    filter.groupBy("cancellation_policy")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    //flexible,moderate, strict_14_with_grace_period içermeli

    filter.groupBy("require_guest_profile_picture")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    //t,f içermeli

    filter.groupBy("require_guest_phone_verification")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    //t,f içermeli

    filter.groupBy("calculated_host_listings_count")
      .count()
      .orderBy(desc("count"))
      .show(50,false)
 //Sayıları içermeli
    filter.groupBy("reviews_per_month")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    //0.18 0.08 gibi sayıları içermeli gerisini uçur

    filter.groupBy("month")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    //1-12 arası sayıları içermeli

  }
}

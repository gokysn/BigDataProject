package org.ytumatmuhproject


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import vegas._
import vegas.render.WindowRenderer._
import vegas.sparkExt._

object DataVisualization {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Spark Session oluşturma
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("DataVisualization")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "2g")
      .getOrCreate()


    val sc = spark.sparkContext

    val nonNull = spark.read.format("csv")
      .option("sep", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:/Users/gokys/Desktop/airbnb/nonNull.csv")


    nonNull.printSchema()

    val minimum_nights = nonNull
      .groupBy("minimum_nights")
      .count()
    println(minimum_nights.count())

    Vegas("minimum_nights", width = 600.0, height = 500.0)
      .withDataFrame(minimum_nights)
      .encodeX("minimum_nights", Quant)
      .encodeY("count", Nom)
      .mark(Bar)
      .show

    val maximum_nights = nonNull
      .groupBy("maximum_nights")
      .count()
    println(maximum_nights.count())

    Vegas("maximum_nights", width = 200.0, height = 500.0)
      .withDataFrame(maximum_nights)
      .encodeX("maximum_nights", Nom)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    val availability_90 = nonNull
      .groupBy("availability_90")
      .count()
    println(availability_90.count())

    Vegas("availability_90", width = 500.0, height = 500.0)
      .withDataFrame(availability_90)
      .encodeX("availability_90", Quant)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    val availability_60 = nonNull
      .groupBy("availability_60")
      .count()
    println(availability_60.count())

    Vegas("availability_60", width = 500.0, height = 500.0)
      .withDataFrame(availability_60)
      .encodeX("availability_60", Nominal)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    val availability_30 = nonNull
      .groupBy("availability_30")
      .count()
    println(availability_30.count())

    Vegas("availability_30", width = 500.0, height = 500.0)
      .withDataFrame(availability_30)
      .encodeX("availability_30", Nominal)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    val guests_included = nonNull
      .groupBy("guests_included")
      .count()
    println(guests_included.count())


    Vegas("guests_included", width = 500.0, height = 500.0)
      .withDataFrame(guests_included)
      .encodeX("guests_included", Nominal)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    val beds = nonNull
      .groupBy("beds")
      .count()

    println(beds.count())

    val bedrooms = nonNull
      .groupBy("bedrooms")
      .count()
    println(beds.count())

    Vegas("bedrooms", width = 500.0, height = 500.0)
      .withDataFrame(bedrooms)
      .encodeX("bedrooms", Nominal)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    Vegas("beds", width = 500.0, height = 500.0)
      .withDataFrame(beds)
      .encodeX("beds", Nominal)
      .encodeY("count", Quant)
      .mark(Bar)
      .show


    val host_total_listings_count = nonNull
      .groupBy("host_total_listings_count")
      .count()
    println(host_total_listings_count.count())


    Vegas("host_total_listings_count", width = 500.0, height = 580.0)
      .withDataFrame(host_total_listings_count)
      .encodeX("host_total_listings_count", Quant)
      .encodeY("count", Nom)
      .mark(Bar)
      .show

    val number_of_reviews = nonNull
      .groupBy("number_of_reviews")
      .count()
    println(number_of_reviews.count())


    Vegas("number_of_reviews", width = 500.0, height = 500.0)
      .withDataFrame(number_of_reviews)
      .encodeX("number_of_reviews", Quant)
      .encodeY("count", Quant)
      .mark(Point)
      .show

    val accommodates = nonNull
      .groupBy("accommodates")
      .count()
    println(accommodates.count())


    Vegas("accommodates", width = 500.0, height = 500.0)
      .withDataFrame(accommodates)
      .encodeX("accommodates", Nominal)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    val availability_365 = nonNull
      .groupBy("availability_365")
      .count()
    println(availability_365.count())


    Vegas("availability_365", width = 500.0, height = 500.0)
      .withDataFrame(availability_365)
      .encodeX("availability_365", Quant)
      .encodeY("count", Quant)
      .mark(Point)
      .show

    val calculated_host_listings_count = nonNull
      .groupBy("calculated_host_listings_count")
      .count()
    println(calculated_host_listings_count.count())


    Vegas("calculated_host_listings_count", width = 500.0, height = 500.0)
      .withDataFrame(calculated_host_listings_count)
      .encodeX("calculated_host_listings_count", Quant)
      .encodeY("count", Quant)
      .mark(Line)
      .show

    val host_listings_count = nonNull
      .groupBy("host_listings_count")
      .count()
    println(host_listings_count.count())

    Vegas("host_listings_count", width = 500.0, height = 500.0)
      .withDataFrame(host_listings_count)
      .encodeX("host_listings_count", Quant)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    Vegas("host_listings_count", width = 500.0, height = 500.0)
      .withDataFrame(host_listings_count)
      .encodeX("host_listings_count", Quant)
      .encodeY("count", Quant)
      .mark(Line)
      .show

    val bathroom = nonNull
      .groupBy("bathrooms")
      .count()
    println(bathroom.count())


    Vegas("Bathrooms", width = 500.0, height = 500.0)
      .withDataFrame(bathroom)
      .encodeX("bathrooms", Quant)
      .encodeY("count", Quant)
      .mark(Point)
      .show


    val monthCount = nonNull
      .groupBy("month")
      .count()
    println(monthCount.count())

    Vegas("Mont", width = 500.0, height = 500.0)
      .withDataFrame(monthCount)
      .encodeX("month", Quant)
      .encodeY("count", Quant)
      .mark(Area) // Change to .mark(Area)
      .show

    Vegas("Mont", width = 500.0, height = 500.0)
      .withDataFrame(monthCount)
      .encodeX("month", Quant)
      .encodeY("count", Quant)
      .mark(Line) // Change to .mark(Area)
      .show

    /* Vegas("accommodates", width = 500.0, height = 500.0)
       .withDataFrame(accommodates)
       .encodeX("accommodates", Nominal)
       .encodeY("count", Quant)
       .mark(Area)
       .show

     Vegas("accommodates", width = 500.0, height = 500.0)
       .withDataFrame(accommodates)
       .encodeX("accommodates", Nominal)
       .encodeY("count", Quant)
       .mark(Line)
       .show

     Vegas("accommodates", width = 500.0, height = 500.0)
       .withDataFrame(accommodates)
       .encodeX("accommodates", Nominal)
       .encodeY("count", Quant)
       .mark(Point)
       .show

     Vegas("availability_365", width = 500.0, height = 500.0)
       .withDataFrame(availability_365)
       .encodeX("availability_365", Nom)
       .encodeY("count", Quant)
       .mark(Bar)
       .show

     Vegas("availability_365", width = 500.0, height = 500.0)
       .withDataFrame(availability_365)
       .encodeX("availability_365", Quant)
       .encodeY("count", Quant)
       .mark(Area)
       .show

     Vegas("availability_365", width = 500.0, height = 500.0)
       .withDataFrame(availability_365)
       .encodeX("availability_365", Quant)
       .encodeY("count", Quant)
       .mark(Line)
       .show


     Vegas("calculated_host_listings_count", width = 500.0, height = 500.0)
       .withDataFrame(calculated_host_listings_count)
       .encodeX("calculated_host_listings_count", Quant)
       .encodeY("count", Quant)
       .mark(Bar)
       .show

     Vegas("calculated_host_listings_count", width = 500.0, height = 500.0)
       .withDataFrame(calculated_host_listings_count)
       .encodeX("calculated_host_listings_count", Quant)
       .encodeY("count", Quant)
       .mark(Area)
       .show

     Vegas("calculated_host_listings_count", width = 500.0, height = 500.0)
       .withDataFrame(calculated_host_listings_count)
       .encodeX("calculated_host_listings_count", Quant)
       .encodeY("count", Quant)
       .mark(Point)
       .show

     Vegas("host_listings_count", width = 500.0, height = 500.0)
       .withDataFrame(host_listings_count)
       .encodeX("host_listings_count", Quant)
       .encodeY("count", Quant)
       .mark(Area)
       .show

     Vegas("host_listings_count", width = 500.0, height = 500.0)
       .withDataFrame(host_listings_count)
       .encodeX("host_listings_count", Quant)
       .encodeY("count", Quant)
       .mark(Point)
       .show

     Vegas("Bathrooms", width = 500.0, height = 500.0)
       .withDataFrame(bathroom)
       .encodeX("bathrooms", Quant)
       .encodeY("count", Quant)
       .mark(Bar)
       .show

     Vegas("Bathrooms", width = 500.0, height = 500.0)
       .withDataFrame(bathroom)
       .encodeX("bathrooms", Quant)
       .encodeY("count", Quant)
       .mark(Area)
       .show

     Vegas("Bathrooms", width = 500.0, height = 500.0)
       .withDataFrame(bathroom)
       .encodeX("bathrooms", Quant)
       .encodeY("count", Quant)
       .mark(Line)
       .show

     Vegas("Mont", width = 500.0, height = 500.0)
       .withDataFrame(priceCount)
       .encodeX("month", Quant)
       .encodeY("count", Quant)
       .mark(Bar) // Change to .mark(Area)
       .show


     Vegas("Mont", width = 500.0, height = 500.0)
       .withDataFrame(priceCount)
       .encodeX("month", Quant)
       .encodeY("count", Quant)
       .mark(Point) // Change to .mark(Area)
       .show*/
  }
}

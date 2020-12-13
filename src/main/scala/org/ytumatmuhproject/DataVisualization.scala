package org.ytumatmuhproject


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
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

    val priceCount = nonNull
      .groupBy("month")
      .count()

    val bathroom = nonNull
      .groupBy("bathrooms")
      .count()

    val host_listings_count = nonNull
      .groupBy("host_listings_count")
      .count()


    // priceCount.show(false)
    //priceCount.printSchema()
    Vegas("host_listings_count", width = 500.0, height = 500.0)
      .withDataFrame(bathroom)
      .encodeX("host_listings_count", Nom)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    Vegas("host_listings_count", width = 500.0, height = 500.0)
      .withDataFrame(bathroom)
      .encodeX("host_listings_count", Nom)
      .encodeY("count", Quant)
      .mark(Area)
      .show

    Vegas("host_listings_count", width = 500.0, height = 500.0)
      .withDataFrame(bathroom)
      .encodeX("host_listings_count", Nom)
      .encodeY("count", Quant)
      .mark(Line)
      .show

    Vegas("host_listings_count", width = 500.0, height = 500.0)
      .withDataFrame(bathroom)
      .encodeX("host_listings_count", Nom)
      .encodeY("count", Quant)
      .mark(Point)
      .show

    Vegas("Bathrooms", width = 500.0, height = 500.0)
      .withDataFrame(bathroom)
      .encodeX("bathrooms", Nom)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    Vegas("Bathrooms", width = 500.0, height = 500.0)
      .withDataFrame(bathroom)
      .encodeX("bathrooms", Nom)
      .encodeY("count", Quant)
      .mark(Area)
      .show

    Vegas("Bathrooms", width = 500.0, height = 500.0)
      .withDataFrame(bathroom)
      .encodeX("bathrooms", Nom)
      .encodeY("count", Quant)
      .mark(Line)
      .show

    Vegas("Bathrooms", width = 500.0, height = 500.0)
      .withDataFrame(bathroom)
      .encodeX("bathrooms", Nom)
      .encodeY("count", Quant)
      .mark(Point)
      .show

    Vegas("Mont", width = 500.0, height = 500.0)
      .withDataFrame(priceCount)
      .encodeX("month", Nom)
      .encodeY("count", Quant)
      .mark(Bar) // Change to .mark(Area)
      .show

    Vegas("Mont", width = 500.0, height = 500.0)
      .withDataFrame(priceCount)
      .encodeX("month", Nom)
      .encodeY("count", Quant)
      .mark(Area) // Change to .mark(Area)
      .show

    Vegas("Mont", width = 500.0, height = 500.0)
      .withDataFrame(priceCount)
      .encodeX("month", Nom)
      .encodeY("count", Quant)
      .mark(Line) // Change to .mark(Area)
      .show

    Vegas("Mont", width = 500.0, height = 500.0)
      .withDataFrame(priceCount)
      .encodeX("month", Nom)
      .encodeY("count", Quant)
      .mark(Point) // Change to .mark(Area)
      .show

  }
}

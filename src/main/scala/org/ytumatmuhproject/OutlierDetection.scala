package org.ytumatmuhproject


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}
import vegas.sparkExt.VegasSpark
import vegas.{Bar, Nom, Quant, Vegas}
import vegas._
import vegas.render.WindowRenderer._


object OutlierDetection {

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

    val outlierDetection = spark.read.format("csv")
      .option("sep", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:/Users/gokys/Desktop/airbnb/nonNull.csv")

    outlierDetection.printSchema()

    val monthCount = outlierDetection
      .groupBy("month")
      .count()
    monthCount.printSchema()


    val month_schema = StructType(Array(
      StructField("month", IntegerType),
      StructField("count", LongType)))


    val selectedMonthDF = monthCount.select("month", "count")
    val month_count = spark.createDataFrame(selectedMonthDF.rdd, month_schema)

    month_count.describe("count").show()

    val monthMedianAndQuantiles = month_count.stat.approxQuantile("count",
      Array(0.25, 0.5, 0.75), 0.0)

    println(monthMedianAndQuantiles.toList)

    //IQR value
    val monthIQR = monthMedianAndQuantiles(2) - monthMedianAndQuantiles(0)
    println("IQR is " + monthIQR)

    val monthLowerRange = monthMedianAndQuantiles(0) - (1.5 * monthIQR)
    val monthUpperRange = monthMedianAndQuantiles(2) + (1.5 * monthIQR)

    val monthOutliers = monthCount.filter(s"count < $monthLowerRange or count > $monthUpperRange")
    monthOutliers.show()

    Vegas("month", width = 600.0, height = 500.0)
      .withDataFrame(monthOutliers)
      .encodeX("month", Nominal)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    println(monthOutliers.count())

    val bedsCount = outlierDetection
      .groupBy("beds")
      .count()
    bedsCount.printSchema()


    val beds_schema = StructType(Array(
      StructField("beds", DoubleType),
      StructField("count", LongType)))


    val selectedBedsDf = bedsCount.select("beds", "count")
    val beds_count = spark.createDataFrame(selectedBedsDf.rdd, beds_schema)

    beds_count.describe("count").show()

    val bedsMedianAndQuantiles = beds_count.stat.approxQuantile("count",
      Array(0.25, 0.5, 0.75), 0.0)

    println(bedsMedianAndQuantiles.toList)

    // Calculate Quantiles and IQR value
    val IQR = bedsMedianAndQuantiles(2) - bedsMedianAndQuantiles(0)
    println("IQR is " + IQR)

    // Filter Outliers

    val lowerRange = bedsMedianAndQuantiles(0) - 1.5 * IQR
    val upperRange = bedsMedianAndQuantiles(2) + 1.5 * IQR

    val bedsOutliers = bedsCount.filter(s"count < $lowerRange or count > $upperRange")

    bedsOutliers.show()

    println(bedsOutliers.count())


    Vegas("beds", width = 600.0, height = 500.0)
      .withDataFrame(bedsOutliers)
      .encodeX("beds", Nominal)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    val bathroomCount = outlierDetection
      .groupBy("bathrooms")
      .count()
    bathroomCount.printSchema()


    val bathroom_schema = StructType(Array(
      StructField("bathrooms", DoubleType),
      StructField("count", LongType)))


    val selectedBathroomDf = bathroomCount.select("bathrooms", "count")
    val bathroom_count = spark.createDataFrame(selectedBathroomDf.rdd, bathroom_schema)

    bathroom_count.describe("count").show()

    val bathroomMedianAndQuantiles = bathroom_count.stat.approxQuantile("count",
      Array(0.25, 0.5, 0.75), 0.0)

    println(bathroomMedianAndQuantiles.toList)

    // Calculate Quantiles and IQR value
    val bathroomIQR = bathroomMedianAndQuantiles(2) - bathroomMedianAndQuantiles(0)
    println("IQR is " + bathroomIQR)

    // Filter Outliers

    val bathroomLowerRange = bathroomMedianAndQuantiles(0) - 1.5 * bathroomIQR
    val bathroomUpperRange = bathroomMedianAndQuantiles(2) + 1.5 * bathroomIQR

    val bathroomOutliers = bathroomCount.filter(s"count < $bathroomLowerRange or count > $bathroomUpperRange")

    bathroomOutliers.show()

    println(bathroomOutliers.count())


    Vegas("bathrooms", width = 600.0, height = 500.0)
      .withDataFrame(bathroomOutliers)
      .encodeX("bathrooms", Nominal)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    val host_listings_count = outlierDetection
      .groupBy("host_listings_count")
      .count()
    host_listings_count.printSchema()


    val host_listings_count_schema = StructType(Array(
      StructField("host_listings_count", DoubleType),
      StructField("count", LongType)))


    val selectedHostListingsDf = host_listings_count.select("host_listings_count", "count")
    val hostListingsCount = spark.createDataFrame(selectedHostListingsDf.rdd, host_listings_count_schema)

    hostListingsCount.describe("count").show()

    val hostListingsMedianAndQuantiles = hostListingsCount.stat.approxQuantile("count",
      Array(0.25, 0.5, 0.75), 0.0)

    println(hostListingsMedianAndQuantiles.toList)

    // Calculate Quantiles and IQR value
    val hostListingsIQR = hostListingsMedianAndQuantiles(2) - hostListingsMedianAndQuantiles(0)
    println("IQR is " + hostListingsIQR)

    // Filter Outliers

    val hostListingsLowerRange = hostListingsMedianAndQuantiles(0) - 1.5 * hostListingsIQR
    val hostListingsUpperRange = hostListingsMedianAndQuantiles(2) + 1.5 * hostListingsIQR

    val hostListingsOutliers = host_listings_count.filter(s"count < $hostListingsLowerRange or count > $hostListingsUpperRange")

    hostListingsOutliers.show()

    println(hostListingsOutliers.count())


    Vegas("host_listings_count", width = 600.0, height = 500.0)
      .withDataFrame(hostListingsOutliers)
      .encodeX("host_listings_count", Nominal)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    val calculated_host_listings_count = outlierDetection
      .groupBy("calculated_host_listings_count")
      .count()
    calculated_host_listings_count.printSchema()


    val calculated_host_listings_schema = StructType(Array(
      StructField("calculated_host_listings_count", IntegerType),
      StructField("count", LongType)))


    val selectedCalculatedHostListingsDF = calculated_host_listings_count.select("calculated_host_listings_count", "count")
    val calculated_host_listings = spark.createDataFrame(selectedCalculatedHostListingsDF.rdd, calculated_host_listings_schema)

    calculated_host_listings.describe("count").show()

    val calculatedMedianAndQuantiles = calculated_host_listings.stat.approxQuantile("count",
      Array(0.25, 0.5, 0.75), 0.0)

    println(calculatedMedianAndQuantiles.toList)

    //IQR value
    val calculatedHostIQR = calculatedMedianAndQuantiles(2) - calculatedMedianAndQuantiles(0)
    println("IQR is " + calculatedHostIQR)

    val calculatedHostLowerRange = calculatedMedianAndQuantiles(0) - 1.5 * calculatedHostIQR
    val calculatedHostUpperRange = calculatedMedianAndQuantiles(2) + 1.5 * calculatedHostIQR

    val calculatedHost = calculated_host_listings_count.filter(s"count < $calculatedHostLowerRange or count > $calculatedHostUpperRange")
    calculatedHost.show()

    println(calculatedHost.count())

    Vegas("calculated_host_listings_count", width = 600.0, height = 500.0)
      .withDataFrame(calculatedHost)
      .encodeX("calculated_host_listings_count", Nominal)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    val availability_365 = outlierDetection
      .groupBy("availability_365")
      .count()
    availability_365.printSchema()


    val availability_365_schema = StructType(Array(
      StructField("availability_365", IntegerType),
      StructField("count", LongType)))


    val selectedAvailability_365_schemaDF = availability_365.select("availability_365", "count")
    val availability365 = spark.createDataFrame(selectedAvailability_365_schemaDF.rdd, availability_365_schema)

    availability365.describe("count").show()

    val availability365MedianAndQuantiles = availability365.stat.approxQuantile("count",
      Array(0.25, 0.5, 0.75), 0.0)

    println(availability365MedianAndQuantiles.toList)

    //IQR value
    val availability365_IQR = availability365MedianAndQuantiles(2) - availability365MedianAndQuantiles(0)
    println("IQR is " + availability365_IQR)

    val availability365LowerRange = availability365MedianAndQuantiles(0) - 1.5 * availability365_IQR
    val availability365UpperRange = availability365MedianAndQuantiles(2) + 1.5 * availability365_IQR

    val available_365 = availability_365.filter(s"count < $availability365LowerRange or count > $availability365UpperRange")
    available_365.show()
    println(available_365.count())

    Vegas("availability_365", width = 900.0, height = 500.0)
      .withDataFrame(available_365)
      .encodeX("availability_365", Nominal)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    val availability_90 = outlierDetection
      .groupBy("availability_90")
      .count()
    availability_90.printSchema()


    val availability_90_schema = StructType(Array(
      StructField("availability_90", IntegerType),
      StructField("count", LongType)))


    val selectedAvailability_90_schemaDF = availability_90.select("availability_90", "count")
    val availability90 = spark.createDataFrame(selectedAvailability_90_schemaDF.rdd, availability_90_schema)

    availability90.describe("count").show()

    val availability90MedianAndQuantiles = availability90.stat.approxQuantile("count",
      Array(0.25, 0.5, 0.75), 0.0)

    println(availability90MedianAndQuantiles.toList)

    //IQR value
    val availability90_IQR = availability90MedianAndQuantiles(2) - availability90MedianAndQuantiles(0)
    println("IQR is " + availability90_IQR)

    val availability90LowerRange = availability90MedianAndQuantiles(0) - 1.5 * availability90_IQR
    val availability90UpperRange = availability90MedianAndQuantiles(2) + 1.5 * availability90_IQR

    val available_90 = availability_90.filter(s"count < $availability90LowerRange or count > $availability90UpperRange")
    available_90.show()
    println(available_90.count())


    Vegas("availability_90", width = 600.0, height = 500.0)
      .withDataFrame(available_90)
      .encodeX("availability_90", Nominal)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    val availability_60 = outlierDetection
      .groupBy("availability_60")
      .count()
    availability_60.printSchema()


    val availability_60_schema = StructType(Array(
      StructField("availability_60", IntegerType),
      StructField("count", LongType)))


    val selectedAvailability_60_schemaDF = availability_60.select("availability_60", "count")
    val availability60 = spark.createDataFrame(selectedAvailability_60_schemaDF.rdd, availability_60_schema)

    availability60.describe("count").show()

    val availability60MedianAndQuantiles = availability60.stat.approxQuantile("count",
      Array(0.25, 0.5, 0.75), 0.0)

    println(availability60MedianAndQuantiles.toList)

    //IQR value
    val availability60_IQR = availability60MedianAndQuantiles(2) - availability60MedianAndQuantiles(0)
    println("IQR is " + availability60_IQR)

    val availability60LowerRange = availability60MedianAndQuantiles(0) - 1.5 * availability60_IQR
    val availability60UpperRange = availability60MedianAndQuantiles(2) + 1.5 * availability60_IQR

    val available_60 = availability_60.filter(s"count < $availability60LowerRange or count > $availability60UpperRange")
    available_60.show()
    println(available_60.count())


    Vegas("availability_60", width = 600.0, height = 500.0)
      .withDataFrame(available_60)
      .encodeX("availability_60", Nominal)
      .encodeY("count", Quant)
      .mark(Bar)
      .show


    val availability_30 = outlierDetection
      .groupBy("availability_30")
      .count()
    availability_30.printSchema()


    val availability_30_schema = StructType(Array(
      StructField("availability_30", IntegerType),
      StructField("count", LongType)))


    val selectedAvailability_30_schemaDF = availability_30.select("availability_30", "count")
    val availability30 = spark.createDataFrame(selectedAvailability_30_schemaDF.rdd, availability_30_schema)

    availability30.describe("count").show()

    val availability30MedianAndQuantiles = availability30.stat.approxQuantile("count",
      Array(0.25, 0.5, 0.75), 0.0)

    println(availability30MedianAndQuantiles.toList)

    //IQR value
    val availability30_IQR = availability30MedianAndQuantiles(2) - availability30MedianAndQuantiles(0)
    println("IQR is " + availability30_IQR)

    val availability30LowerRange = availability30MedianAndQuantiles(0) - 1.5 * availability30_IQR
    val availability30UpperRange = availability30MedianAndQuantiles(2) + 1.5 * availability30_IQR

    val available_30 = availability_30.filter(s"count < $availability30LowerRange or count > $availability30UpperRange")
    available_30.show()
    println(available_30.count())

    Vegas("availability_30", width = 600.0, height = 500.0)
      .withDataFrame(available_30)
      .encodeX("availability_30", Nominal)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    val accommodates = outlierDetection
      .groupBy("accommodates")
      .count()
    accommodates.printSchema()


    val accommodates_schema = StructType(Array(
      StructField("accommodates", IntegerType),
      StructField("count", LongType)))


    val selectedAccommodatesDF = accommodates.select("accommodates", "count")
    val accommodatesSummary = spark.createDataFrame(selectedAccommodatesDF.rdd, accommodates_schema)

    accommodatesSummary.describe("count").show()

    val accommodatesMedianAndQuantiles = accommodatesSummary.stat.approxQuantile("count",
      Array(0.25, 0.5, 0.75), 0.0)

    println(accommodatesMedianAndQuantiles.toList)

    //IQR value
    val accommodates_IQR = accommodatesMedianAndQuantiles(2) - accommodatesMedianAndQuantiles(0)
    println("IQR is " + accommodates_IQR)

    val accommodatesLowerRange = accommodatesMedianAndQuantiles(0) - 1.5 * accommodates_IQR
    val accommodatesUpperRange = accommodatesMedianAndQuantiles(2) + 1.5 * accommodates_IQR

    val accommodatesOutliers = accommodates.filter(s"count < $accommodatesLowerRange or count > $accommodatesUpperRange")
    accommodatesOutliers.show()
    println(accommodatesOutliers.count())

    Vegas("accommodates", width = 600.0, height = 500.0)
      .withDataFrame(accommodatesOutliers)
      .encodeX("accommodates", Nominal)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    val number_of_reviews = outlierDetection
      .groupBy("number_of_reviews")
      .count()
    number_of_reviews.printSchema()


    val number_of_reviews_schema = StructType(Array(
      StructField("number_of_reviews", IntegerType),
      StructField("count", LongType)))


    val selectedNumberOfReviewsDF = number_of_reviews.select("number_of_reviews", "count")
    val numberOfReviews = spark.createDataFrame(selectedNumberOfReviewsDF.rdd, number_of_reviews_schema)

    numberOfReviews.describe("count").show()

    val numberOfReviewsMedianAndQuantiles = numberOfReviews.stat.approxQuantile("count",
      Array(0.25, 0.5, 0.75), 0.0)

    println(numberOfReviewsMedianAndQuantiles.toList)

    //IQR value
    val numberOfReviews_IQR = numberOfReviewsMedianAndQuantiles(2) - numberOfReviewsMedianAndQuantiles(0)
    println("IQR is " + numberOfReviews_IQR)

    val numberOfReviewsLowerRange = numberOfReviewsMedianAndQuantiles(0) - 1.5 * numberOfReviews_IQR
    val numberOfReviewsUpperRange = numberOfReviewsMedianAndQuantiles(2) + 1.5 * numberOfReviews_IQR

    val numberOfReviewsOutliers = number_of_reviews.filter(s"count < $numberOfReviewsLowerRange or count > $numberOfReviewsUpperRange")
    numberOfReviewsOutliers.show()
    println(numberOfReviewsOutliers.count())

    Vegas("number_of_reviews", width = 600.0, height = 500.0)
      .withDataFrame(numberOfReviewsOutliers)
      .encodeX("number_of_reviews", Nominal)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    val bedrooms = outlierDetection
      .groupBy("bedrooms")
      .count()
    bedrooms.printSchema()


    val bedrooms_schema = StructType(Array(
      StructField("bedrooms", DoubleType),
      StructField("count", LongType)))


    val selectedBedroomsDF = bedrooms.select("bedrooms", "count")
    val bedroomsSummary = spark.createDataFrame(selectedBedroomsDF.rdd, bedrooms_schema)

    bedroomsSummary.describe("count").show()

    val bedroomsMedianAndQuantiles = bedroomsSummary.stat.approxQuantile("count",
      Array(0.25, 0.5, 0.75), 0.0)

    println(bedroomsMedianAndQuantiles.toList)

    //IQR value
    val bedrooms_IQR = bedroomsMedianAndQuantiles(2) - bedroomsMedianAndQuantiles(0)
    println("IQR is " + bedrooms_IQR)

    val bedroomsLowerRange = bedroomsMedianAndQuantiles(0) - 1.5 * bedrooms_IQR
    val bedroomsUpperRange = bedroomsMedianAndQuantiles(2) + 1.5 * bedrooms_IQR

    val bedroomsOutliers = bedrooms.filter(s"count < $bedroomsLowerRange or count > $bedroomsUpperRange")
    bedroomsOutliers.show()
    println(bedroomsOutliers.count())

    Vegas("bedrooms", width = 600.0, height = 500.0)
      .withDataFrame(bedroomsOutliers)
      .encodeX("bedrooms", Nominal)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    val host_total_listings_count = outlierDetection
      .groupBy("host_total_listings_count")
      .count()
    host_total_listings_count.printSchema()


    val host_total_listings_count_schema = StructType(Array(
      StructField("host_total_listings_count", DoubleType),
      StructField("count", LongType)))


    val selectedhostTotalListingsDF = host_total_listings_count.select("host_total_listings_count", "count")
    val hostTotalListingsSummary = spark.createDataFrame(selectedhostTotalListingsDF.rdd, host_total_listings_count_schema)

    hostTotalListingsSummary.describe("count").show()

    val hostTotalListingsMedianAndQuantiles = hostTotalListingsSummary.stat.approxQuantile("count",
      Array(0.25, 0.5, 0.75), 0.0)

    println(hostTotalListingsMedianAndQuantiles.toList)

    //IQR value
    val hostTotalListings_IQR = hostTotalListingsMedianAndQuantiles(2) - hostTotalListingsMedianAndQuantiles(0)
    println("IQR is " + hostTotalListings_IQR)

    val hostTotalListingsLowerRange = hostTotalListingsMedianAndQuantiles(0) - 1.5 * hostTotalListings_IQR
    val hostTotalListingsUpperRange = hostTotalListingsMedianAndQuantiles(2) + 1.5 * hostTotalListings_IQR

    val host_total_listings_countOutliers = host_total_listings_count.filter(s"count < $hostTotalListingsLowerRange or count > $hostTotalListingsUpperRange")
    host_total_listings_countOutliers.show()
    println(host_total_listings_countOutliers.count())

    Vegas("host_total_listings_count", width = 600.0, height = 500.0)
      .withDataFrame(host_total_listings_countOutliers)
      .encodeX("host_total_listings_count", Nominal)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    val guests_included = outlierDetection
      .groupBy("guests_included")
      .count()
    guests_included.printSchema()


    val guests_included_schema = StructType(Array(
      StructField("guests_included", IntegerType),
      StructField("count", LongType)))


    val selectedguestsIncludedDF = guests_included.select("guests_included", "count")
    val guestsIncluded = spark.createDataFrame(selectedguestsIncludedDF.rdd, guests_included_schema)

    guestsIncluded.describe("count").show()

    val guestsIncludedMedianAndQuantiles = guestsIncluded.stat.approxQuantile("count",
      Array(0.25, 0.5, 0.75), 0.0)

    println(guestsIncludedMedianAndQuantiles.toList)

    //IQR value
    val guestsIncluded_IQR = guestsIncludedMedianAndQuantiles(2) - guestsIncludedMedianAndQuantiles(0)
    println("IQR is " + guestsIncluded_IQR)

    val guestsIncludedLowerRange = guestsIncludedMedianAndQuantiles(0) - 1.5 * guestsIncluded_IQR
    val guestsIncludedUpperRange = guestsIncludedMedianAndQuantiles(2) + 1.5 * guestsIncluded_IQR

    val guestsIncludedOutliers = guests_included.filter(s"count < $guestsIncludedLowerRange or count > $guestsIncludedUpperRange")
    guestsIncludedOutliers.show()
    println(guestsIncludedOutliers.count())

    Vegas("guests_included", width = 600.0, height = 500.0)
      .withDataFrame(guestsIncludedOutliers)
      .encodeX("guests_included", Nominal)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    val maximum_nights = outlierDetection
      .groupBy("maximum_nights")
      .count()
    maximum_nights.printSchema()


    val maximumNightsDF_schema = StructType(Array(
      StructField("maximum_nights", IntegerType),
      StructField("count", LongType)))


    val selectedMaximumNightsDF = maximum_nights.select("maximum_nights", "count")
    val maximumNights = spark.createDataFrame(selectedMaximumNightsDF.rdd, maximumNightsDF_schema)

    maximumNights.describe("count").show()

    val maximumNightsMedianAndQuantiles = maximumNights.stat.approxQuantile("count",
      Array(0.25, 0.5, 0.75), 0.0)

    println(maximumNightsMedianAndQuantiles.toList)

    //IQR value
    val maximumNights_IQR = maximumNightsMedianAndQuantiles(2) - maximumNightsMedianAndQuantiles(0)
    println("IQR is " + maximumNights_IQR)

    val maximumNightsLowerRange = maximumNightsMedianAndQuantiles(0) - 1.5 * maximumNights_IQR
    val maximumNightsUpperRange = maximumNightsMedianAndQuantiles(2) + 1.5 * maximumNights_IQR

    val maximumNightsOutliers = maximum_nights.filter(s"count < $maximumNightsLowerRange or count > $maximumNightsUpperRange")
    maximumNightsOutliers.show()
    println(maximumNightsOutliers.count())

    Vegas("maximum_nights", width = 600.0, height = 500.0)
      .withDataFrame(maximumNightsOutliers)
      .encodeX("maximum_nights", Nominal)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

    val minimum_nights = outlierDetection
      .groupBy("minimum_nights")
      .count()
    minimum_nights.printSchema()


    val minimumNightsDF_schema = StructType(Array(
      StructField("minimum_nights", IntegerType),
      StructField("count", LongType)))


    val selectedMinimumNightsDF = minimum_nights.select("minimum_nights", "count")
    val minimumNights = spark.createDataFrame(selectedMinimumNightsDF.rdd, minimumNightsDF_schema)

    minimumNights.describe("count").show()

    val minimumNightsMedianAndQuantiles = minimumNights.stat.approxQuantile("count",
      Array(0.25, 0.5, 0.75), 0.0)

    println(minimumNightsMedianAndQuantiles.toList)

    //IQR value
    val minimumNights_IQR = minimumNightsMedianAndQuantiles(2) - minimumNightsMedianAndQuantiles(0)
    println("IQR is " + minimumNights_IQR)

    val minimumNightsLowerRange = minimumNightsMedianAndQuantiles(0) - 1.5 * minimumNights_IQR
    val minimumNightsUpperRange = minimumNightsMedianAndQuantiles(2) + 1.5 * minimumNights_IQR

    val minimumNightsOutliers = minimum_nights.filter(s"count < $minimumNightsLowerRange or count > $minimumNightsUpperRange")
    minimumNightsOutliers.show()
    println(minimumNightsOutliers.count())

    Vegas("minimum_nights", width = 600.0, height = 500.0)
      .withDataFrame(minimumNightsOutliers)
      .encodeX("minimum_nights", Nominal)
      .encodeY("count", Quant)
      .mark(Bar)
      .show

  }
}

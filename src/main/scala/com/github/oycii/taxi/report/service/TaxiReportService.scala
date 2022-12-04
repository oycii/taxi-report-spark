package com.github.oycii.taxi.report.service

import com.github.oycii.taxi.report.entity.TaxiTime
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, count, desc, format_number}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}

import java.sql.Timestamp

object TaxiReportService extends LazyLogging{

  def popDistricts(dfTaxiRide: DataFrame, dfTaxiZone: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val dfMax = dfTaxiRide.groupBy(col("PULocationID"))
      .agg(count(col("passenger_count")))
      .select(col("PULocationID"), col("count(passenger_count)").as("passenger_count"))
    val dfReport = dfMax.join(dfTaxiZone, dfMax("PULocationID") === dfTaxiZone("LocationID"), "left_outer")
      .orderBy(col("passenger_count").desc)

    dfReport.show()
    dfReport
  }

  def popTime(ds: Dataset[(Int, Int)]): RDD[String] = {
    ds.rdd.reduceByKey(_ + _).sortBy(_._2).map(rec => rec._1 + " " + rec._2)
  }

  def popDistances(dfTaxiRide: DataFrame)(implicit spark: SparkSession): DataFrame = {
    dfTaxiRide.withColumn("trep_distance_meters", col("trip_distance") * 1000)
      .withColumn("distance", (col("trep_distance_meters") / 1000).cast("integer"))
      .groupBy("distance").agg(
        count(col("distance")),
        format_number(functions.min(col("trip_distance")), 1).as("min"),
        format_number(functions.max(col("trip_distance")), 1).as("max"),
        format_number(functions.avg(col("trip_distance")), 1).as("avg"),
        format_number(functions.stddev(col("trip_distance")), 1).as("stddev")
      )
      .orderBy(col("count(distance)").desc)
      .withColumn("from_distance_km", col("distance"))
      .withColumn("to_distance_km", col("distance") + 1)
  }
}

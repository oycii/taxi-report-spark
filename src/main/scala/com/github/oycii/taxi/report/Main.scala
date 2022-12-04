package com.github.oycii.taxi.report

import com.github.oycii.taxi.report.dict.Consts.{DATA_TAXI_POP_DISTRICTS, DATA_TAXI_RAID, DATA_TAXI_TIME, DATA_TAXI_ZONE, DB_PROPERTIES, DB_TABLE_NAME, DB_URL}
import com.github.oycii.taxi.report.dict.Format.{csv, parquet}
import com.github.oycii.taxi.report.service.{DataService, TaxiReportService, ViewService}
import com.github.oycii.taxi.report.utils.SparkUtils
import com.typesafe.scalalogging.LazyLogging
import org.apache.log4j.Level
import org.apache.log4j.Logger.getRootLogger
import org.apache.spark.storage.StorageLevel

import java.util.Properties
import scala.io.Source

object Main extends LazyLogging {
  def main(args: Array[String]): Unit = {
    getRootLogger().setLevel(Level.OFF)
    SparkUtils.withSparkSession("taxi-report-spark", implicit spark => {
      val dataService = new DataService
      val dfTaxiRide = dataService.getData(DATA_TAXI_RAID, parquet).persist(StorageLevel.MEMORY_AND_DISK)

      val dfTaxiZone = dataService.getData(DATA_TAXI_ZONE, csv).persist(StorageLevel.MEMORY_AND_DISK)

      val dfTaxiPopDistrict = TaxiReportService.popDistricts(dfTaxiRide, dfTaxiZone)
      dataService.saveData(dfTaxiPopDistrict, DATA_TAXI_POP_DISTRICTS)

      val rddPopTaxiTime = dataService.getTexiTimeList(dfTaxiRide)
      val reportTaxiTime = TaxiReportService.popTime(rddPopTaxiTime)
      ViewService.viewTaxiTime(reportTaxiTime)
      dataService.saveData(reportTaxiTime, DATA_TAXI_TIME)

      val dfTaxiDistances = TaxiReportService.popDistances(dfTaxiRide)
      dataService.saveData(dfTaxiDistances, DB_URL, DB_TABLE_NAME, DB_PROPERTIES)

    })
    logger.info("Application finished")
  }
}

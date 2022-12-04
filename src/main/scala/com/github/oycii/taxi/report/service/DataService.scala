package com.github.oycii.taxi.report.service

import com.github.oycii.taxi.report.dao.DataDao
import com.github.oycii.taxi.report.dict.Format
import com.github.oycii.taxi.report.entity.TaxiTime
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

import java.sql.Timestamp
import java.util.Properties

class DataService(implicit spark: SparkSession) extends LazyLogging {
  val dataDao = new DataDao(spark)

  def getData(path: String, format: Format.Value): DataFrame = {
    val df = dataDao.getDF(path, format: Format.Value)
    logger.info("path: " + path)
    df
  }

  def getTexiTimeList(df: DataFrame): Dataset[(Int, Int)] = {
    df.map(row => (row.getTimestamp(2).getHours, 1))(Encoders.product[(Int, Int)])
  }

  def saveData(df: DataFrame, path: String): Unit = {
    dataDao.saveDataFrame(df, path)
  }

  def saveData(rdd: RDD[String], path: String): Unit = {
    dataDao.saveData(rdd, path)
  }

  def saveData(df: DataFrame, url: String, tableName: String, props: Properties): Unit = {
    dataDao.saveData(df, url, tableName, props)
  }

}

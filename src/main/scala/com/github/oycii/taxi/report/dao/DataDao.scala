package com.github.oycii.taxi.report.dao

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.github.oycii.taxi.report.dict.Format
import org.apache.spark.rdd.RDD

import scala.reflect.io.Directory
import java.io.File
import java.util.Properties

class DataDao(spark: SparkSession) {

  def getDF(path: String, format: Format.Value): DataFrame = {
    val df = format match {
      case Format.parquet => spark.read.load(path)
      case Format.csv => spark.read.format("csv").option("header", "true").load(path)
      case Format.json => spark.read.format("json").load(path)
      case _ => throw new RuntimeException("Not support forma: " + format)
    }

    df
  }

  def saveDataFrame(df: DataFrame, path: String): Unit = {
    df.coalesce(1).write.option("maxRecordsPerFile", 100000).mode("overwrite").parquet(path)
  }

  def dropDirectory(path: String): Unit = {
    val file = new File(path.replace("file:", ""))
    if (file.isDirectory) {
      val dir = new Directory(file)
      if (dir.isDirectory)
        dir.deleteRecursively()
    }
  }

  def saveData(rdd: RDD[String], path: String): Unit = {
    dropDirectory(path)
    rdd.coalesce(1).saveAsTextFile(path)
  }

  def saveData(df: DataFrame, url: String, tableName: String, prop: Properties): Unit = {
    df.coalesce(1).write.jdbc(url=url, table=tableName, prop)
  }
}

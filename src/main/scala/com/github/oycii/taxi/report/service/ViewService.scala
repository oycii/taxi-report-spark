package com.github.oycii.taxi.report.service

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD

object ViewService extends LazyLogging {
  def viewTaxiTime(rdd: RDD[String]): Unit = {
    rdd.foreach(rec => println(rec))
  }
}

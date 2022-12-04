package com.github.oycii.taxi.report.dict

import scala.collection.JavaConverters._
import java.util.Properties

object Format extends Enumeration {
  val parquet = Value("parquet")
  val csv = Value("csv")
  val json = Value("json")
}

object Consts {
  val DATA_TAXI_RAID = "file:////home/sanya/projects/otus/taxi-report-spark/src/main/resources/data/yellow_taxi_jan_25_2018"
  val DATA_TAXI_ZONE = "file://///home/sanya/projects/otus/taxi-report-spark/src/main/resources/data/taxi_zones.csv"
  val DATA_TAXI_POP_DISTRICTS = "file://///home/sanya/projects/otus/taxi-report-spark/taxi-pop-districts.parquet"
  val DATA_TAXI_TIME = "file://///home/sanya/projects/otus/taxi-report-spark/taxi-time.txt"
  val DB_URL = "jdbc:postgresql://localhost:5432/taxi"
  val properties = Map("user" -> "docker", "password" -> "docker", "driver" -> "org.postgresql.Driver")
  val DB_PROPERTIES = new Properties()
  properties.foreach { case (key, value) => DB_PROPERTIES.setProperty(key, value.toString) }
  val DB_TABLE_NAME = "taxi_report"
}

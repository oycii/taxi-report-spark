package com.github.oycii.taxi.report.service

import org.apache.spark.sql.test.SharedSparkSession
import com.sysgears.DataFrameBuilder._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, Encoders, functions}

class TaxiReportServiceSpec extends SharedSparkSession {

  test("Taxi pop districts should be find") {
    val dfTaxiRide: DataFrame =
        ! "PULocationID" | "passenger_count" |
        ! 1              | 1                 |
        ! 1              | 1                 |
        ! 1              | 1                 |
        ! 2              | 1                 |
        ! 2              | 1                 |

    val dfTaxiZone: DataFrame =
      !"LocationID"|      "Borough"|                "Zone"|"service_zone"|
      !           1|          "EWR"|      "Newark Airport"|         "EWR"|
      !           2|       "Queens"|         "Jamaica Bay"|        "BORO"|

    val dfResult = TaxiReportService.popDistricts(dfTaxiRide, dfTaxiZone)

    val dfExpected: DataFrame =
      !"PULocationID"|"passenger_count"|"LocationID"|"Borough"|          "Zone"|"service_zone"|
      !             1|                3|           1|    "EWR"|"Newark Airport"|         "EWR"|
      !             2|                2|           2| "Queens"|   "Jamaica Bay"|        "BORO"|

    val diff = dfResult.except(dfExpected)
      .union(dfExpected.except(dfResult))

    assert(diff.count() == 0)
  }

  test("Pop time should be find") {
    import testImplicits._
    val dfTaxiTime: DataFrame =
      !"time" | "pas_count" |
      !     21|            1|
      !     21|            1|
      !     21|            1|
      !     22|            1|

    val rdd = dfTaxiTime.map(row => (row.getInt(0), row.getInt(1))) (Encoders.product[(Int, Int)])
    val dfPopTime = TaxiReportService.popTime(rdd).toDF

    val dfTaxiPopExpected: DataFrame =
      !"time" | "pas_count" |
      !     21|           3 |
      !     22|           1 |

    val dfExpected = dfTaxiPopExpected
      .withColumn("value", functions.concat(col("time"), lit(" "), col("pas_count")))
      .select(col("value"))

    val diff = dfPopTime.except(dfExpected).union(dfExpected.except(dfPopTime))
    assert(diff.count() == 0)
  }

  test("Taxi pop distances should be find") {
    val dfTaxiDistances: DataFrame =
      !"trip_distance" |
      !            0.1 |
      !            0.1 |
      !            0.1 |
      !            2.0 |
      !            2.0 |

    val dfResult = TaxiReportService.popDistances(dfTaxiDistances)

    val dfExpected: DataFrame =
      !"distance" | "count(distance)" | "min" | "max"  | "avg"  | "stddev"  |"from_distance_km"|"to_distance_km"|
      !          0|                  3|    0.1|     0.1|     0.1|        0.0|                 0|               1|
      !          2|                  2|    2.0|     2.0|     2.0|        0.0|                 2|               3|

    val diff = dfResult.except(dfExpected)
      .union(dfExpected.except(dfResult))

    assert(diff.count() == 0)
  }

}

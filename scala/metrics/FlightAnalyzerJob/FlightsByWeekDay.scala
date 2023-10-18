package com.example
package metrics.FlightAnalyzerJob

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, avg}

import transformers.DataFrameTransform
import schemas.FlightAnalyzerJob.SchemasFlightAnalyzer._

object FlightsByWeekDay {

  def calculate(df: DataFrame,
                sortType: String): DataFrame = {

    val description =
      "Дни недели в порядке своевременности прибытия рейсов, совершаемых в эти дни"

    val avgArrivalDelayTime = avg(columnArrivalDelayTime).as(columnCount)

    val flightsByWeekDayDF = DataFrameTransform(df)
      .groupByColumns(
        columnWeekDay,
        columnArrivalDelayTime,
        avgArrivalDelayTime
      )
      .withColumnFillValue(
        columnDescription,
        description
      )
      .sortBy(
        columnCount,
        sortType
      )
      .toDataFrame

    flightsByWeekDayDF
  }
}

package com.example
package metrics.FlightAnalyzerJob

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count}

import transformers.DataFrameTransform
import transformers.SortType
import schemas.FlightAnalyzerJob.SchemasFlightAnalyzer._

object Top10AirportsByCountFlights {
  def calculate(df: DataFrame,
                sortType: String): DataFrame = {

    val description =
      "Топ-10 самых популярных аэропортов по количеству совершаемых полетов"

    val metricFunc = count(columnFlightNumber).as(columnCount)

    val top10AirportsByCountFlightsDF = DataFrameTransform(df)
      .groupByColumns(
        newColumnAirportName,
        columnFlightNumber,
        metricFunc
      )
      .top_n(
        columnCount,
        topColumn,
        sortType
      )
      .withColumnFillValue(
        columnDescription,
        description
      )
      .selectStringColumns(
        resultColumnsTop10AirportsByCountFlights
      )
      .sortBy(
        topColumn,
        SortType.ascSortType
      )
      .toDataFrame

    top10AirportsByCountFlightsDF
  }
}

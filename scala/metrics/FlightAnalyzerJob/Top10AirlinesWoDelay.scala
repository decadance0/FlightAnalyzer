package com.example
package metrics.FlightAnalyzerJob

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count}

import transformers.{DataFrameTransform, SortType}
import schemas.FlightAnalyzerJob.SchemasFlightAnalyzer._

object Top10AirlinesWoDelay {
  def calculate(df: DataFrame,
                sortType: String): DataFrame = {
    val description =
      "Топ-10 авиакомпаний, вовремя выполняющих рейсы"

    val conditionWoDelay = col(columnArrivalDelayTime) <= 0

    val countFlightNumber = count(columnFlightNumber).as(columnCount)

    val top10AirlinesWoDelayDF = DataFrameTransform(df)
      .filterByColumn(conditionWoDelay)
      .groupByColumns(
        columnAirlinesName,
        columnFlightNumber,
        countFlightNumber
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
        resultColumnsTop10AirlinesWoDelay
      )
      .sortBy(
        topColumn,
        SortType.ascSortType
      )
      .toDataFrame

    top10AirlinesWoDelayDF
  }
}

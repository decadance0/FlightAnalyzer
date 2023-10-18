package com.example
package metrics.FlightAnalyzerJob

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{asc, col, count, desc, row_number}
import org.apache.spark.sql.expressions.Window

import transformers.DataFrameTransform
import transformers.SortType
import schemas.FlightAnalyzerJob.SchemasFlightAnalyzer._

object Top10AirlinesByAirportsOrigin {

  def calculate(df: DataFrame,
                sortType: String): DataFrame = {

    val description =
      "Топ-10 перевозчиков на основании вовремя совершенного вылета"

    val columnsAgg = List(newColumnAirportName, newColumnAirportNameDestination, columnAirlinesName)

    val conditionWoDelay = col(columnDelayTime) <= 0
    val conditionLimit10 = col(topColumn) <= 10

    val countFlightNumber = count(columnFlightNumber).as(columnCount)

    val sortColumns =
      List(
        SortType.sortTypeMatch(sortType, columnCount),
        SortType.sortTypeMatch(sortType, newColumnAirportNameDestination)
      )

    val windowByAirportName = Window
      .partitionBy(newColumnAirportName)
      .orderBy(sortColumns: _*)

    val rowNumberColumn =
      row_number()
        .over(windowByAirportName)
        .as(topColumn)

    val top10AirlinesByAirportsOriginDF = DataFrameTransform(df)
      .filterByColumn(conditionWoDelay)
      .groupByColumns(
        columnsAgg,
        List(columnFlightNumber),
        countFlightNumber
      )
      .selectColumns(
        List(
          col(newColumnAirportName),
          col(newColumnAirportNameDestination),
          col(columnAirlinesName),
          col(columnCount),
          rowNumberColumn
        )
      )
      .filterByColumn(
        conditionLimit10
      )
      .withColumnFillValue(
        columnDescription,
        description
      )
      .selectStringColumns(
        resultColumnsTop10AirlinesByAirportsOrigin
      )
      .sortByList(
        List(newColumnAirportName, topColumn),
        SortType.ascSortType
      )
      .toDataFrame

    top10AirlinesByAirportsOriginDF
  }

}

package com.example
package metrics.FlightAnalyzerJob

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, sum, max}
import org.apache.spark.sql.expressions.Window

import transformers.DataFrameTransform
import schemas.FlightAnalyzerJob.SchemasFlightAnalyzer._

object CntFlightsByDelayReason {

  def meltDelayReason(df: DataFrame, delayReason: String): DataFrame = {

    val filterNotZero = col(columnValue) > 0

    DataFrameTransform(df)
      .meltColumn(
        columnDelayReason,
        columnValue,
        delayReason
      )
      .filterByColumn(filterNotZero)
      .selectStringColumns(List(columnDelayReason, columnValue))
      .toDataFrame
  }

  def calculate(df: DataFrame,
                sortType: String): DataFrame = {

    val description =
      "Кол-во и процент времени об общих задержанных рейсов по причинам"

    val columnsDelayReason: List[String] = List(
      columnAirSystemDelay,
      columnSecurityDelay,
      columnAirlineDelay,
      columnLateAircraftDelay,
      columnWeatherDelay
    )

    val newColumnDelayReason = columnDelayReason + "_1"

    val conditionDelayedFlights = col(columnDelayTime) > 0

    val emptyWindowSpec = Window.partitionBy()

    val metricCountFlights = count(columnValue).as(columnCountFlights)

    val metricPercentDelayTimeOfTotal =
      (sum(columnValue).as(columnSumDelayTime) / max(col(columnSum)) * 100)
        .as(columnPercentOfTotal)

    val filteredDF = DataFrameTransform(df)
      .selectStringColumns(columnsDelayReason)
      .filterByColumn(conditionDelayedFlights)
      .toDataFrame

    val meltedDF =
      meltDelayReason(filteredDF, columnAirSystemDelay)
        .union(
          meltDelayReason(filteredDF, columnSecurityDelay)
        )
        .union(
          meltDelayReason(filteredDF, columnAirlineDelay)
        )
        .union(
          meltDelayReason(filteredDF, columnLateAircraftDelay)
        )
        .union(
          meltDelayReason(filteredDF, columnWeatherDelay)
        )

    val cntFlightsByDelayReasonDF = DataFrameTransform(meltedDF)
      .groupByColumns(
        columnDelayReason,
        columnValue,
        metricCountFlights
      )
      .toDataFrame

    val percentDelayTimeOfTotalDF = DataFrameTransform(meltedDF)
      .withColumnFillMeasure(
        columnSum,
        sum(columnValue).over(emptyWindowSpec)
      )
      .groupByColumns(
        List(columnDelayReason),
        List(columnValue, columnSum),
        metricPercentDelayTimeOfTotal
      )
      .renameColumn(
        columnDelayReason,
        newColumnDelayReason
      )
      .toDataFrame

    val conditionJoin =
      cntFlightsByDelayReasonDF(columnDelayReason) ===
        percentDelayTimeOfTotalDF(newColumnDelayReason)

    val joinedDF = DataFrameTransform(cntFlightsByDelayReasonDF)
      .joinDF(
        percentDelayTimeOfTotalDF,
        conditionJoin
      )
      .dropColumn(newColumnDelayReason)
      .withColumnFillValue(
        columnDescription,
        description
      )
      .sortBy(
        columnCountFlights,
        sortType
      )
      .toDataFrame

    joinedDF

  }

}

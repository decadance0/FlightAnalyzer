package com.example
package metrics.FlightAnalyzerJob

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, max, sum, when}
import org.apache.spark.sql.expressions.Window
import transformers.DataFrameTransform
import schemas.FlightAnalyzerJob.SchemasFlightAnalyzer._

object CntFlightsByDelayReason {

  private def meltDelayReason(df: DataFrame): DataFrame = {

    def meltOneDelayReason(oneDelayReasonDF: DataFrame, delayReason: String): DataFrame = {
      val filterNotZero = col(columnValue) > 0

      DataFrameTransform(oneDelayReasonDF)
        .meltColumn(
          columnDelayReason,
          columnValue,
          delayReason
        )
        .filterByColumn(filterNotZero)
        .selectStringColumns(List(columnDelayReason, columnValue))
        .toDataFrame
    }

    meltOneDelayReason(df, columnAirSystemDelay)
      .union(
        meltOneDelayReason(df, columnSecurityDelay)
      )
      .union(
        meltOneDelayReason(df, columnAirlineDelay)
      )
      .union(
        meltOneDelayReason(df, columnLateAircraftDelay)
      )
      .union(
        meltOneDelayReason(df, columnWeatherDelay)
      )
  }

  def calculate(df: DataFrame,
                sortType: String): DataFrame = {

    val description =
      "Кол-во и процент времени об общих задержанных рейсов по причинам"

    val conditionDelayedFlights = col(columnDelayTime) > 0

    // Фильтруем датафрейм
    val filteredDF = DataFrameTransform(df)
      .selectStringColumns(columnsDelayReason)
      .filterByColumn(conditionDelayedFlights)
      .toDataFrame

    // Функции агрегации
    val metricSumDelayTime = columnsDelayReason.map { colName =>
      sum(when(col(colName) > 0, col(colName)).otherwise(0)).as(colName)
    }

    val metricCntFlights = columnsDelayReason.map { colName =>
      sum(when(col(colName) > 0, 1)).as(colName)
    }

    // Агрегируем датафреймы
    val sumDelayTimeDF = DataFrameTransform(filteredDF)
      .aggColumns(metricSumDelayTime)
      .toDataFrame

    val cntFlightsByDelayReasonDF = DataFrameTransform(filteredDF)
      .aggColumns(metricCntFlights)
      .toDataFrame

    // Формула подсчета процента от общего времени
    val metricPercentDelayTimeOfTotal =
      (sum(columnValue).as(columnSumDelayTime) / max(col(columnSum)) * 100)
        .as(columnPercentOfTotal)

    // Пустая оконка
    val emptyWindowSpec = Window.partitionBy()

    // Новое имя для колонки, чтобы потом ее удалить
    val newColumnDelayReason: String = columnDelayReason + "_1"

    // Транспонируем датафреймы
    val meltedSumDelayTimeDF = meltDelayReason(sumDelayTimeDF)
    val meltedCntFlightsByDelayReasonDF = DataFrameTransform(
      meltDelayReason(cntFlightsByDelayReasonDF)
    )
      .renameColumn(columnValue, columnCountFlights)
      .toDataFrame

    // Считаем процент от общего времени для каждого типа задержки рейса
    val percentDelayTimeOfTotalDF = DataFrameTransform(meltedSumDelayTimeDF)
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

    // Условие соединения датафреймов
    val conditionJoin =
      meltedCntFlightsByDelayReasonDF(columnDelayReason) ===
        percentDelayTimeOfTotalDF(newColumnDelayReason)

    // Соединяем два датафрейма в один
    val joinedDF = DataFrameTransform(meltedCntFlightsByDelayReasonDF)
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

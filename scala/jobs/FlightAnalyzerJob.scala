package com.example
package jobs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

import readers.{CsvReader, ReaderFactory}
import writers.{ConsoleWriter, WriterFactory}
import transformers.{DataFrameTransform, SortType}
import schemas.FlightAnalyzerJob.SchemasFlightAnalyzer._
import tools.AnalysisMetadata

import metrics.FlightAnalyzerJob.Top10AirportsByCountFlights
import metrics.FlightAnalyzerJob.Top10AirlinesWoDelay
import metrics.FlightAnalyzerJob.Top10AirlinesByAirportsOrigin
import metrics.FlightAnalyzerJob.FlightsByWeekDay
import metrics.FlightAnalyzerJob.CntFlightsByDelayReason


case class FlightAnalyzerJobConfig(airlinesReaderConfig: CsvReader.ReaderConfig,
                                   airportsReaderConfig: CsvReader.ReaderConfig,
                                   flightsReaderConfig: CsvReader.ReaderConfig,
                                   writerConfig: ConsoleWriter.WriterConfig,
                                   sortMetricType: String = SortType.ascSortType)

object DataFrameNames {
  // Исходные датафреймы
  val airlines: String = "airlines"
  val airports: String = "airports"
  val flights: String = "flights"

  // Метрики
  val top10AirportsByCountFlights: String = "Top10AirportsByCountFlights"
  val top10AirlinesWoDelay: String = "Top10AirlinesWoDelay"
  val top10AirlinesByAirportsOrigin: String = "Top10AirlinesByAirportsOrigin"
  val flightsByWeekDay: String = "FlightsByWeekDay"
  val cntFlightsByDelayReason: String = "CntFlightsByDelayReason"
}

class FlightAnalyzerJob(spark: SparkSession,
                        flightAnalyzerJobConfig: FlightAnalyzerJobConfig) extends Job {

  var analysisMetadata: AnalysisMetadata.AnalysisMetadataConfig = _

  override def read(): Map[String, DataFrame] = {

    val airlinesDF: DataFrame = ReaderFactory
      .createReader(
        spark,
        flightAnalyzerJobConfig.airlinesReaderConfig
      )
      .read()

    val airportsDF: DataFrame = ReaderFactory
      .createReader(
        spark,
        flightAnalyzerJobConfig.airportsReaderConfig
      )
      .read()

    val flightsDF: DataFrame = ReaderFactory
      .createReader(
        spark,
        flightAnalyzerJobConfig.flightsReaderConfig
      )
      .read()

    Map(
      DataFrameNames.airlines -> airlinesDF,
      DataFrameNames.airports -> airportsDF,
      DataFrameNames.flights -> flightsDF
    )
  }

  override def transform(data: Any): DataFrame = {

    def filterFlights(flightsDF: DataFrame): DataFrame = {

      val conditionIsNotCanceled = col(columnCanceled) === 0

      DataFrameTransform(flightsDF)
        .filterByColumn(conditionIsNotCanceled)
        .toDataFrame
    }

    def joinDFs(airlinesDF: DataFrame,
                airportsDF: DataFrame,
                flightsDF: DataFrame): DataFrame = {

      val joinCondFlightsAirportsOrigin = col(columnIATACode) === col(columnOriginAirport)
      val joinCondFlightsAirportsDestination = col(columnIATACode) === col(columnDestinationAirport)
      val joinCondFlightsAirlines = col(columnAirlinesCode) === col(columnIATACodeAirlines)

      val joinedAirportOriginDF = DataFrameTransform(flightsDF)
        .joinDF(
          airportsDF,
          joinCondFlightsAirportsOrigin
        )
        .renameColumn(
          columnAirportName,
          newColumnAirportName
        )
        .dropColumn(
          columnIATACode
        )
        .dropColumn(
          columnOriginAirport
        )
        .toDataFrame

      val joinedAirportDestinationDF = DataFrameTransform(joinedAirportOriginDF)
        .joinDF(
          airportsDF,
          joinCondFlightsAirportsDestination
        )
        .renameColumn(
          columnAirportName,
          newColumnAirportNameDestination
        )
        .dropColumn(
          columnIATACode
        )
        .dropColumn(
          columnDestinationAirport
        )
        .toDataFrame

      val joinedAirlinesDF = DataFrameTransform(joinedAirportDestinationDF)
        .joinDF(
          airlinesDF,
          joinCondFlightsAirlines
        )
        .dropColumn(columnIATACodeAirlines)
        .toDataFrame

      joinedAirlinesDF
    }

    def processing(airlinesDF: DataFrame,
                   airportsDF: DataFrame,
                   flightsDF: DataFrame): DataFrame = {
      // Фильтруем датафрейм
      val filteredFlightsDF = filterFlights(flightsDF)

      // Соединяем все датафреймы
      val joinedDF = joinDFs(airlinesDF, airportsDF, filteredFlightsDF)

      // Добавляем колонку с датой
      val addDateColumnDF = DataFrameTransform(joinedDF)
        .parseDate(
          columnYear,
          columnMonth,
          columnDay,
          columnDate
        )
        .toDataFrame

      // Получаем экстремумы дат
      val startDate = DataFrameTransform(addDateColumnDF)
        .getExtremumDate(
          columnDate,
          maxDate = false
        )

      val endDate = DataFrameTransform(addDateColumnDF)
        .getExtremumDate(
          columnDate
        )

      // Записываем метаданные в конфиг
      setAnalysisMetadata(
        AnalysisMetadata.AnalysisMetadataConfig(
          startDate = startDate,
          endDate = endDate
        )
      )

      addDateColumnDF
    }

    data match {
      case dataMap: Map[String, DataFrame] => {
        processing(
          dataMap(DataFrameNames.airlines),
          dataMap(DataFrameNames.airports),
          dataMap(DataFrameNames.flights)
        )
      }
      case _ => throw new IllegalArgumentException("Invalid data format")
    }
  }

  override def calculate_metrics(transformedDF: DataFrame): Map[String, DataFrame] = {
    val top10AirportsByCountFlightsDF = Top10AirportsByCountFlights.calculate(
      transformedDF,
      flightAnalyzerJobConfig.sortMetricType
    )

//    val top10AirlinesWoDelayDF = Top10AirlinesWoDelay.calculate(
//      transformedDF,
//      flightAnalyzerJobConfig.sortMetricType
//    )
//
//    val top10AirlinesByAirportsOriginDF = Top10AirlinesByAirportsOrigin.calculate(
//      transformedDF,
//      flightAnalyzerJobConfig.sortMetricType
//    )
//
//    val flightsByWeekDayDF = FlightsByWeekDay.calculate(
//      transformedDF,
//      flightAnalyzerJobConfig.sortMetricType
//    )
//
//    val cntFlightsByDelayReasonDF = CntFlightsByDelayReason.calculate(
//      transformedDF,
//      flightAnalyzerJobConfig.sortMetricType
//    )

    Map(
      DataFrameNames.top10AirportsByCountFlights -> top10AirportsByCountFlightsDF,
//      DataFrameNames.top10AirlinesWoDelay -> top10AirlinesWoDelayDF,
//      DataFrameNames.top10AirlinesByAirportsOrigin -> top10AirlinesByAirportsOriginDF,
//      DataFrameNames.flightsByWeekDay -> flightsByWeekDayDF,
//      DataFrameNames.cntFlightsByDelayReason -> cntFlightsByDelayReasonDF
    )
  }

  override def write(metricsDFs: Map[String, DataFrame]): Unit = {

    WriterFactory.createWriter(
      metricsDFs(DataFrameNames.top10AirportsByCountFlights),
      flightAnalyzerJobConfig.writerConfig
    )
      .write()

//    WriterFactory.createWriter(
//      metricsDFs(DataFrameNames.top10AirlinesWoDelay),
//      flightAnalyzerJobConfig.writerConfig
//    )
//      .write()
//
//    WriterFactory.createWriter(
//      metricsDFs(DataFrameNames.top10AirlinesByAirportsOrigin),
//      flightAnalyzerJobConfig.writerConfig
//    )
//      .write()
//
//    WriterFactory.createWriter(
//      metricsDFs(DataFrameNames.flightsByWeekDay),
//      flightAnalyzerJobConfig.writerConfig
//    )
//      .write()
//
//    WriterFactory.createWriter(
//      metricsDFs(DataFrameNames.cntFlightsByDelayReason),
//      flightAnalyzerJobConfig.writerConfig
//    )
//      .write()
  }

}

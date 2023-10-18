package com.example

import readers.CsvReader
import transformers.SortType
import writers.ConsoleWriter

import jobs.{FlightAnalyzerJob, FlightAnalyzerJobConfig}
import schemas.FlightAnalyzerJob.SchemasFlightAnalyzer.{schemaAirlines, schemaAirports, schemaFlights}

object FlightAnalyzer extends SessionWrapper {

  def main(args: Array[String]): Unit = {

    val airlinesPath = "src/main/resources/src/flight_analyzer_job/airlines.csv"
    val airportsPath = "src/main/resources/src/flight_analyzer_job/airports.csv"
    val flightsPath = "src/main/resources/src/flight_analyzer_job/flights.csv"

//    val airlinesPath = args(0)
//    val airportsPath = args(1)
//    val flightsPath = args(2)

    val jobFlightAnalyzer = new FlightAnalyzerJob(
      spark = spark,
      FlightAnalyzerJobConfig(
        airlinesReaderConfig = CsvReader.ReaderConfig(
           filePath = airlinesPath,
          schema = schemaAirlines
        ),
        airportsReaderConfig = CsvReader.ReaderConfig(
          filePath = airportsPath,
          schema = schemaAirports
        ),
        flightsReaderConfig = CsvReader.ReaderConfig(
          filePath = flightsPath,
          schema = schemaFlights
        ),
        writerConfig = ConsoleWriter.WriterConfig(),
        sortMetricType = SortType.descSortType
      )
    )

    jobFlightAnalyzer.run()
  }

}

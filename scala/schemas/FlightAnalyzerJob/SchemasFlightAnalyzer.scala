package com.example
package schemas.FlightAnalyzerJob

import org.apache.spark.sql.types._

object SchemasFlightAnalyzer {

  val schemaAirlines: StructType = StructType(Seq(
    StructField("IATA_CODE_AIRLINES", StringType),
    StructField("AIRLINE_NAME", StringType)
  ))

  val schemaAirports: StructType = StructType(Seq(
    StructField("IATA_CODE_AIRPORTS", StringType),
    StructField("AIRPORT_NAME", StringType)
    //    Ненужные колонки
    //    ,
    //    StructField("CITY", StringType),
    //    StructField("STATE", StringType),
    //    StructField("COUNTRY", StringType),
    //    StructField("LATITUDE", IntegerType),
    //    StructField("LONGITUDE", IntegerType)
  ))

  val schemaFlights: StructType = StructType(Seq(
    StructField("YEAR", IntegerType),
    StructField("MONTH", IntegerType),
    StructField("DAY", IntegerType),
    StructField("DAY_OF_WEEK", IntegerType),
    StructField("AIRLINE", StringType),
    StructField("FLIGHT_NUMBER", IntegerType),
    StructField("TAIL_NUMBER", StringType),
    StructField("ORIGIN_AIRPORT", StringType),
    StructField("DESTINATION_AIRPORT", StringType),
    StructField("SCHEDULED_DEPARTURE", StringType),
    StructField("DEPARTURE_TIME", StringType),
    StructField("DEPARTURE_DELAY", StringType),
    StructField("TAXI_OUT", StringType),
    StructField("WHEELS_OFF", StringType),
    StructField("SCHEDULED_TIME", IntegerType),
    StructField("ELAPSED_TIME", IntegerType),
    StructField("AIR_TIME", IntegerType),
    StructField("DISTANCE", IntegerType),
    StructField("WHEELS_ON", IntegerType),
    StructField("TAXI_IN", IntegerType),
    StructField("SCHEDULED_ARRIVAL", IntegerType),
    StructField("ARRIVAL_TIME", IntegerType),
    StructField("ARRIVAL_DELAY", IntegerType),
    StructField("DIVERTED", IntegerType),
    StructField("CANCELLED", IntegerType),
    StructField("CANCELLATION_REASON", StringType),
    StructField("AIR_SYSTEM_DELAY", IntegerType),
    StructField("SECURITY_DELAY", IntegerType),
    StructField("AIRLINE_DELAY", IntegerType),
    StructField("LATE_AIRCRAFT_DELAY", IntegerType),
    StructField("WEATHER_DELAY", IntegerType)
  ))

  //  Колонки для метрик
  val topColumn: String = "TOP_N"
  val columnDescription: String = "DESCRIPTION"
  val columnCount = "COUNT"
  val columnDelayReason = "DELAY_REASON"
  val columnValue = "VALUE"
  val columnSumDelayTime = "DELAY_TIME"
  val columnPercentOfTotal = "PERCENT_OF_TOTAL"
  val columnSum = "SUM"
  val columnCountFlights = "COUNT_FLIGHTS"
  val columnDate = "DATE"

  // Используемые колонки из Flights
  val columnYear: String = schemaFlights.head.name
  val columnMonth: String = schemaFlights(1).name
  val columnDay: String = schemaFlights(2).name
  val columnWeekDay: String = schemaFlights(3).name
  val columnAirlinesCode: String = schemaFlights(4).name
  val columnFlightNumber: String = schemaFlights(5).name
  val columnOriginAirport: String = schemaFlights(7).name
  val columnDestinationAirport: String = schemaFlights(8).name
  val columnDelayTime: String = schemaFlights(11).name
  val columnArrivalDelayTime: String = schemaFlights(22).name
  val columnCanceled: String = schemaFlights(24).name

  val columnAirSystemDelay: String = schemaFlights(26).name
  val columnSecurityDelay: String = schemaFlights(27).name
  val columnAirlineDelay: String = schemaFlights(28).name
  val columnLateAircraftDelay: String = schemaFlights(29).name
  val columnWeatherDelay: String = schemaFlights(30).name

  val columnsDelayReason: List[String] = List(
    columnAirSystemDelay,
    columnSecurityDelay,
    columnAirlineDelay,
    columnLateAircraftDelay,
    columnWeatherDelay
  )

  // Используемые колонки из Airports
  val columnIATACode: String = schemaAirports(0).name
  val columnAirportName: String = schemaAirports(1).name
  val newColumnAirportName: String = "AIRPORT_NAME_ORIGIN"
  val newColumnAirportNameDestination: String = "AIRPORT_NAME_DESTINATION"

  // Используемые колонки из Airlines
  val columnIATACodeAirlines: String = schemaAirlines(0).name
  val columnAirlinesName: String = schemaAirlines(1).name

  // Колонки в результирующих датафреймах
  val resultColumnsTop10AirportsByCountFlights: List[String] =
    List(topColumn, newColumnAirportName, columnCount, columnDescription)

  val resultColumnsTop10AirlinesWoDelay: List[String] =
    List(topColumn, columnAirlinesName, columnCount, columnDescription)

  val resultColumnsTop10AirlinesByAirportsOrigin: List[String] =
    List(topColumn, newColumnAirportName, newColumnAirportNameDestination, columnAirlinesName, columnCount, columnDescription)

}


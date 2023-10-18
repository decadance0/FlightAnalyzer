package com.example
package readers

import logger.MyLogger
import org.apache.log4j.Logger

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object CsvReader {
  case class ReaderConfig(
    filePath: String,
    schema: StructType,
    separator: Char = ',',
    hasHeader: Boolean = true
  )
}

class CsvReader(spark: SparkSession,
                config: CsvReader.ReaderConfig) extends Reader {

  val logger: Logger = MyLogger.getLogger

  override def read(): DataFrame = {
    try {
      spark.read
        .option("header", config.hasHeader.toString.toLowerCase)
        .option("sep", config.separator.toString)
        .schema(config.schema)
        .csv(config.filePath)
    } catch {
      case e: Exception =>
        logger.error("An error occurred while reading data from CSV:\n" + e.getMessage)
        throw e
    }
  }
}

package com.example
package writers

import logger.MyLogger
import org.apache.log4j.Logger

import org.apache.spark.sql.{DataFrame, SaveMode}

object CsvWriter {
  case class WriterConfig(path: String, mode: SaveMode)
}

class CsvWriter(df: DataFrame, config: CsvWriter.WriterConfig) extends Writer {

  val logger: Logger = MyLogger.getLogger

  override def write(): Unit = {
    try {
      df.write
        .format("csv")
        .mode(config.mode)
        .save(config.path)
    } catch {
      case e: Exception =>
        logger.error("An error occurred while writing data to CSV: " + e.getMessage)
        throw e
    }
  }
}


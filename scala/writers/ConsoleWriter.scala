package com.example
package writers

import logger.MyLogger
import org.apache.log4j.Logger

import org.apache.spark.sql.DataFrame

object ConsoleWriter {
  case class WriterConfig()
}

class ConsoleWriter(df: DataFrame, config: ConsoleWriter.WriterConfig) extends Writer {

  val logger: Logger = MyLogger.getLogger

  override def write(): Unit = {
    try {
      df.show()
    } catch {
      case e: Exception =>
        logger.error("An error occurred while showing dataframe:\n" + e.getMessage)
        throw e
    }
  }
}

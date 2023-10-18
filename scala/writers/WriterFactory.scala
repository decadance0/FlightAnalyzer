package com.example
package writers

import org.apache.spark.sql.DataFrame

object WriterFactory {

  def createWriter(df: DataFrame, writerConfig: CsvWriter.WriterConfig): CsvWriter = {
    new CsvWriter(df: DataFrame, writerConfig)
  }

  def createWriter(df: DataFrame, writerConfig: ConsoleWriter.WriterConfig): ConsoleWriter = {
    new ConsoleWriter(df: DataFrame, writerConfig)
  }
}

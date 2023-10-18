package com.example
package readers

import org.apache.spark.sql.SparkSession

object ReaderFactory {

  def createReader(spark: SparkSession, readerConfig: CsvReader.ReaderConfig): CsvReader = {
    new CsvReader(spark, readerConfig)
  }

//  def createReader(spark: SparkSession, readerConfig: SqlReader.ReaderConfig): SqlReader = {
//    new SqlReader(spark, readerConfig)
//  }
}

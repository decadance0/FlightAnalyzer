//package com.example
//package readers
//
//import logger.MyLogger
//
//import org.apache.spark.sql.{SparkSession, DataFrame}
//import org.apache.spark.sql.types._
//
//object SqlReader {
//  case class ReaderConfig(
//                           connection: String,
//                           query: String
//                         )
//}
//
//class SqlReader(spark: SparkSession,
//                config: SqlReader.ReaderConfig) extends Reader {
//
//  val logger = MyLogger.getLogger
//
//  override def read(): DataFrame = {
//    try {
//      // reading data from database
//    } catch {
//      case e: Exception =>
//        logger.error("An error occurred while reading data from database:\n" + e.getMessage)
//        throw e
//    }
//  }
//
//}

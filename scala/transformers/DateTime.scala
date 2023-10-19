package com.example
package transformers

import logger.MyLogger
import org.apache.log4j.Logger

import java.text.SimpleDateFormat
import java.util.Date

object DateTime {

  private val logger: Logger = MyLogger.getLogger

  def getCurrentTimestamp: String = {
    try {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val currentDate = new Date()
      dateFormat.format(currentDate)
    } catch {
      case e: Exception =>
        logger.error("Error while getting current timestamp:\n" + e.getMessage)
        throw e
    }
  }
}

package com.example
package logger

import org.apache.log4j.{Logger, Level}

object MyLogger {

  private lazy val logger = {
    val log = Logger.getLogger(getClass)
    log.setLevel(Level.ERROR)
    log
  }

  def getLogger: Logger = logger
}


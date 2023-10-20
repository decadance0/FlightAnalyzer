package com.example

import org.apache.spark.sql.SparkSession

import Config.getSparkSession

trait SessionWrapper {

  lazy val appName = "Flight Analyzer App"

  lazy val spark: SparkSession =
    getSparkSession
      .appName(appName)
      .getOrCreate()

}

package com.example

import org.apache.spark.sql.SparkSession

trait SessionWrapper {

  lazy val appName = "Flight Analyzer App"
  lazy val paramMaster = "local"

  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName(appName)
    .master(Config.get("master"))
    .getOrCreate()

}

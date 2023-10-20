package com.example

import org.apache.spark.sql.SparkSession

object Config {
  private val dev: SparkSession.Builder = {
    SparkSession
      .builder()
      .master("local")
  }

  private val prod: SparkSession.Builder = {
    SparkSession
      .builder()
  }

  private val environment = sys.env.getOrElse("PROJECT_ENV", "prod")

  def getSparkSession: SparkSession.Builder = {
    environment match {
      case "dev" => dev
      case _ => prod
    }
  }

  private val path: String = "src/main/resources"

  def getHomePath: String = {
    path
  }

  def getMetaPath: String = {
    path + "/meta"
  }
}
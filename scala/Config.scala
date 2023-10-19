package com.example

object Config {
  private val dev: Map[String, String] = {
    Map("master" -> "local")
  }

  private val prod: Map[String, String] = {
    Map("master" -> "spark://a32643e80e6f:7077")
  }

  private val environment = sys.env.getOrElse("PROJECT_ENV", "prod")

  def get(key: String): String = {
    environment match {
      case "dev" => dev(key)
      case _ => prod(key)
    }
  }

  private val path: String = ""

  def getHomePath: String = {
    path
  }

  def getMetaPath: String = {
    path + "/meta"
  }
}
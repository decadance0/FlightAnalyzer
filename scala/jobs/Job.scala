package com.example
package jobs

import org.apache.spark.sql.DataFrame

trait Job {

  def read(): Any

  def transform(data: Any): DataFrame

  def calculate_metrics(transformedDF: DataFrame): Map[String, DataFrame]

  def write(metricsDFs: Map[String, DataFrame]): Unit

  final def run(): Unit = {
    val data = read()
    val transformedDF = transform(data)
    val metricsDFs = calculate_metrics(transformedDF)
    write(metricsDFs)
  }
}

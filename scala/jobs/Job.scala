package com.example
package jobs

import com.example.logger.MyLogger
import com.example.tools.AnalysisMetadata

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

trait Job {

  final val logger: Logger = MyLogger.getLogger

  var analysisMetadata: AnalysisMetadata.AnalysisMetadataConfig
  final def setAnalysisMetadata(metadata: AnalysisMetadata.AnalysisMetadataConfig): Unit = {
    analysisMetadata = metadata
  }

  def read(): Any

  def transform(data: Any): DataFrame

  def calculate_metrics(transformedDF: DataFrame): Map[String, DataFrame]

  def write(metricsDFs: Map[String, DataFrame]): Unit

  final def run(): Unit = {
    val data = read()
    val transformedDF = transform(data)
    val metricsDFs = calculate_metrics(transformedDF)
    write(metricsDFs)
    AnalysisMetadata.write(
      analysisMetadata
    )

    logger.info("Job has been completed successfully")

  }
}

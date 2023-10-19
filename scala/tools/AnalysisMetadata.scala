package com.example
package tools

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.example.SessionWrapper
import transformers.DateTime
import writers.{CsvWriter, WriterFactory}

object AnalysisMetadata extends SessionWrapper {
  case class AnalysisMetadataConfig(
    startDate: String,
    endDate: String,
  )

  private val schema = new StructType()
    .add(StructField("processed", StringType))
    .add(StructField("collected", StringType))

  def write(config: AnalysisMetadataConfig): Unit = {

    val metadata = Seq(
      Row(DateTime.getCurrentTimestamp, s"${config.startDate} - ${config.endDate}")
    )

    val metaDF = spark
      .createDataFrame(
        spark.sparkContext.parallelize(metadata),
        schema
      )

    WriterFactory.createWriter(
      df = metaDF,
      writerConfig = CsvWriter.WriterConfig(
        path = Config.getMetaPath + "/meta_info.csv",
        mode = SaveMode.Append
      )
    )
      .write()
  }
}

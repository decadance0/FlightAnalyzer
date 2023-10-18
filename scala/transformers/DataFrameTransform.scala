package com.example
package transformers

import org.apache.spark.sql.functions.{col, lit, monotonically_increasing_id}
import org.apache.spark.sql.{Column, DataFrame}

object DataFrameTransform {
  def apply(df: DataFrame): DataFrameTransform = new DataFrameTransform(df)
}

class DataFrameTransform(df: DataFrame) {
  private var currentDF: DataFrame = df

  def filterByColumn(filter: Column): DataFrameTransform = {
    currentDF = currentDF
      .filter(filter)
    this
  }

  def withColumnFillValue(columnName: String,
                          value: String): DataFrameTransform = {
    currentDF = currentDF
      .withColumn(columnName, lit(value))
    this
  }

  def withColumnFillMeasure(columnName: String,
                          value: Column): DataFrameTransform = {
    currentDF = currentDF
      .withColumn(columnName, value)
    this
  }

  def joinDF(rightDF: DataFrame,
             joinCondition: Column,
             joinType: String = "inner"): DataFrameTransform = {
    currentDF = currentDF
      .join(rightDF, joinCondition, joinType)
    this
  }

  def renameColumn(oldNameColumn: String,
                   newNameColumn: String): DataFrameTransform = {
    currentDF = currentDF
      .withColumnRenamed(oldNameColumn, newNameColumn)
    this
  }

  def groupByColumns(aggColumns: List[String],
                     metricColumn: List[String],
                     metricFunc: Column): DataFrameTransform = {
    val selectColumns = metricColumn ::: aggColumns
    currentDF = currentDF
      .select(selectColumns.map(col): _*)
      .groupBy(aggColumns.map(col): _*)
      .agg(metricFunc)
    this
  }

  def groupByColumns(aggColumn: String,
                     metricColumn: String,
                     metricFunc: Column): DataFrameTransform = {
    currentDF = currentDF
      .select(aggColumn, metricColumn)
      .groupBy(aggColumn)
      .agg(metricFunc)
    this
  }

  def top_n(sortColumn: String,
            topColumnName: String,
            sortType: String,
            n: Int = 10): DataFrameTransform = {
    currentDF = currentDF
      .orderBy(SortType.sortTypeMatch(sortType, sortColumn))
      .limit(n)
      .withColumn(topColumnName, monotonically_increasing_id() + 1)
    this
  }

  def selectStringColumns(columns: List[String]): DataFrameTransform = {
    currentDF = currentDF
      .select(columns.map(col): _*)
    this
  }

  def selectColumns(columns: List[Column]): DataFrameTransform = {
    currentDF = currentDF
      .select(columns: _*)
    this
  }

  def sortBy(sortColumn: String, sortType: String): DataFrameTransform = {
    sortType match {
      case "desc" => {
        currentDF = currentDF
          .orderBy(col(sortColumn).desc)
      }
      case "asc" => {
        currentDF = currentDF
          .orderBy(col(sortColumn).asc)
      }
    }
    this
  }

  def sortByList(sortColumns: List[String], sortType: String): DataFrameTransform = {
    val orderByColumns = sortColumns.map(col(_))
    val sortedDF = sortType match {
      case "asc" => currentDF.orderBy(orderByColumns: _*)
      case "desc" => currentDF.orderBy(orderByColumns.map(_.desc): _*)
    }
    currentDF = sortedDF
    this
  }

  def dropColumn(column: String): DataFrameTransform = {
    currentDF = currentDF
      .drop(col(column))
    this
  }

  def meltColumn(attributeColumn: String,
                 valueColumn: String,
                 dataColumn: String): DataFrameTransform = {
    currentDF = currentDF
      .withColumn(attributeColumn, lit(dataColumn))
      .withColumn(valueColumn, col(dataColumn))
    this
  }

  def toDataFrame: DataFrame = {
    currentDF
  }
}

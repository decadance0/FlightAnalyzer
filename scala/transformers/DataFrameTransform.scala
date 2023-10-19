package com.example
package transformers

import org.apache.spark.sql.functions.{col, concat, lit, max, min, monotonically_increasing_id, to_date}
import org.apache.spark.sql.{Column, DataFrame, functions}
import logger.MyLogger

import org.apache.log4j.Logger

object DataFrameTransform {
  def apply(df: DataFrame): DataFrameTransform = new DataFrameTransform(df)
}

class DataFrameTransform(df: DataFrame) {
  private var currentDF: DataFrame = df

  private val logger: Logger = MyLogger.getLogger

  def filterByColumn(filter: Column): DataFrameTransform = {
    try {
      currentDF = currentDF.filter(filter)
      this
    } catch {
      case e: Exception =>
        logger.error("Error while filtering DataFrame:\n" + e.getMessage)
        throw e
    }
  }

  def withColumnFillValue(columnName: String,
                          value: String): DataFrameTransform = {
    try {
      currentDF = currentDF
        .withColumn(columnName, lit(value))
      this
    } catch {
      case e: Exception =>
        logger.error("Error while adding a column with a constant value:\n" + e.getMessage)
        throw e
    }
  }

  def withColumnFillMeasure(columnName: String,
                            value: Column): DataFrameTransform = {
    try {
      currentDF = currentDF
        .withColumn(columnName, value)
      this
    } catch {
      case e: Exception =>
        logger.error("Error while adding a column with a constant value:\n" + e.getMessage)
        throw e
    }
  }

  def joinDF(rightDF: DataFrame,
             joinCondition: Column,
             joinType: String = "inner"): DataFrameTransform = {
    try {
      currentDF = currentDF
        .join(rightDF, joinCondition, joinType)
      this
    } catch {
      case e: Exception =>
        logger.error("Error while joining a DataFrame:\n" + e.getMessage)
        throw e
    }
  }

  def renameColumn(oldNameColumn: String,
                   newNameColumn: String): DataFrameTransform = {
    try {
      currentDF = currentDF
        .withColumnRenamed(oldNameColumn, newNameColumn)
      this
    } catch {
      case e: Exception =>
        logger.error("Error while renaming a column:\n" + e.getMessage)
        throw e
    }
  }

  def groupByColumns(aggColumns: List[String],
                     metricColumn: List[String],
                     metricFunc: Column): DataFrameTransform = {
    try {
      val selectColumns = metricColumn ::: aggColumns
      currentDF = currentDF
        .select(selectColumns.map(col): _*)
        .groupBy(aggColumns.map(col): _*)
        .agg(metricFunc)
      this
    } catch {
      case e: Exception =>
        logger.error("Error while grouping a Dataframe:\n" + e.getMessage)
        throw e
    }
  }

  def groupByColumns(aggColumn: String,
                     metricColumn: String,
                     metricFunc: Column): DataFrameTransform = {
    try {
      currentDF = currentDF
        .select(aggColumn, metricColumn)
        .groupBy(aggColumn)
        .agg(metricFunc)
      this
    } catch {
      case e: Exception =>
        logger.error("Error while grouping a Dataframe:\n" + e.getMessage)
        throw e
    }
  }

  def top_n(sortColumn: String,
            topColumnName: String,
            sortType: String,
            n: Int = 10): DataFrameTransform = {
    try {
      currentDF = currentDF
        .orderBy(SortType.sortTypeMatch(sortType, sortColumn))
        .limit(n)
        .withColumn(topColumnName, monotonically_increasing_id() + 1)
      this
    } catch {
      case e: Exception =>
        logger.error("Error while calculating a top N:\n" + e.getMessage)
        throw e
    }
  }

  def selectStringColumns(columns: List[String]): DataFrameTransform = {
    try {
      currentDF = currentDF
        .select(columns.map(col): _*)
      this
    } catch {
      case e: Exception =>
        logger.error("Error while selecting columns:\n" + e.getMessage)
        throw e
    }
  }

  def selectColumns(columns: List[Column]): DataFrameTransform = {
    try {
      currentDF = currentDF
        .select(columns: _*)
      this
    } catch {
      case e: Exception =>
        logger.error("Error while selecting columns:\n" + e.getMessage)
        throw e
    }
  }

  def sortBy(sortColumn: String, sortType: String): DataFrameTransform = {
    try {
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
    } catch {
      case e: Exception =>
        logger.error("Error while sorting a DataFrame:\n" + e.getMessage)
        throw e
    }
  }

  def sortByList(sortColumns: List[String], sortType: String): DataFrameTransform = {
    try {
      val orderByColumns = sortColumns.map(col(_))
      val sortedDF = sortType match {
        case "asc" => currentDF.orderBy(orderByColumns: _*)
        case "desc" => currentDF.orderBy(orderByColumns.map(_.desc): _*)
      }
      currentDF = sortedDF
      this
    } catch {
      case e: Exception =>
        logger.error("Error while sorting a DataFrame:\n" + e.getMessage)
        throw e
    }
  }

  def dropColumn(column: String): DataFrameTransform = {
    try {
      currentDF = currentDF
        .drop(col(column))
      this
    } catch {
      case e: Exception =>
        logger.error("Error while dropping a column:\n" + e.getMessage)
        throw e
    }
  }

  def meltColumn(attributeColumn: String,
                 valueColumn: String,
                 dataColumn: String): DataFrameTransform = {
    try {
      currentDF = currentDF
        .withColumn(attributeColumn, lit(dataColumn))
        .withColumn(valueColumn, col(dataColumn))
      this
    } catch {
      case e: Exception =>
        logger.error("Error while melting a DataFrame:\n" + e.getMessage)
        throw e
    }
  }

  def aggColumns(aggFunc: List[Column]): DataFrameTransform = {
    try {
      currentDF = currentDF
        .agg(aggFunc.head, aggFunc.tail: _*)
      this
    } catch {
      case e: Exception =>
        logger.error("Error while aggregating columns:\n" + e.getMessage)
        throw e
    }
  }

  def parseDate(yearColumn: String,
                monthColumn: String,
                dayColumn: String,
                dateColumnName: String,
                sep: String = "-"): DataFrameTransform = {
    try {
      currentDF = currentDF
        .withColumn(
          dateColumnName,
          to_date(
            concat(
              col(yearColumn),
              lit(sep),
              col(monthColumn),
              lit(sep),
              col(dayColumn)
            )
          )
        )
      this
    } catch {
      case e: Exception =>
        logger.error("Error while parsing date columns:\n" + e.getMessage)
        throw e
    }
  }

  def getExtremumDate(dateColumnName: String,
                      maxDate: Boolean = true): String = {
    try {
      if (maxDate) {
        df
          .select(col(dateColumnName))
          .agg(max(col(dateColumnName)))
          .collectAsList()
          .get(0)
          .mkString
      } else {
        df
          .select(col(dateColumnName))
          .agg(min(col(dateColumnName)))
          .collectAsList()
          .get(0)
          .mkString
      }
    } catch {
      case e: Exception =>
        logger.error("Error while getting extremum date:\n" + e.getMessage)
        throw e
    }
  }
  def toDataFrame: DataFrame = {
    currentDF
  }
}

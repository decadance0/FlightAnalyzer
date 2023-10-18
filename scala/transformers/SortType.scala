package com.example
package transformers

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object SortType {
  val ascSortType: String = "asc"
  val descSortType: String = "desc"

  def sortTypeMatch(sortType: String, column: String): Column = {
    sortType match {
      case "desc" => col(column).desc
      case "asc" => col(column).asc
    }
  }
}

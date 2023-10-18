package com.example
package readers

import org.apache.spark.sql.DataFrame

object Reader {
  trait ReaderConfig
}

trait Reader {
  def read(): DataFrame
}

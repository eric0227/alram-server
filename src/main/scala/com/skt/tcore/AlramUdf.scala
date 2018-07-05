package com.skt.tcore

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf

object AlramUdf {

  def metricMean = udf((list:Seq[Row]) => {
    val sum = (list map {
      case Row(mean: Double, cnt: Long) => mean * cnt
    }).sum

    val total = (list map {
      case Row(_, cnt: Long) => cnt
    }).sum

    if(total == 0) 0 else sum / total
  })
}

package com.skt.tcore.common

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SQLContext}

object Common {

  def printConsole(df: DataFrame, numRows: Int = 0): Unit = {

    if (df.isStreaming) {

      val writer = df.writeStream.format("console").option("header", "true").option("truncate", false)
      if(numRows > 0) writer.option("numRows", numRows)
      writer.start()
    } else {

      if(numRows > 0) df.show(numRows, truncate=false) else df.show(truncate=false)
    }
  }
}

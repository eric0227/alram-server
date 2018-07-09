package com.skt.tcore

import com.skt.tcore.model.WindowAlarmRule
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.window

object WindowQuery {

  def startWindowQuery(df: DataFrame, rule: WindowAlarmRule): Unit = {
    import df.sparkSession.implicits._

    df.filter(rule.windowFilter.filterStr)
      .withWatermark("timestamp", rule.windowDuration)
      .groupBy(window($"timestamp", rule.windowDuration, rule.slidingDuration))
      .count().as("cnt")
      .writeStream
      .format("console")
      .start()
  }
}

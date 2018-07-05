package com.skt.tcore

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

object EventDetectQuery {

  def query(df: DataFrame): StreamingQuery = {
    import df.sparkSession.implicits._

    // test
    if(AlarmRuleManager.getEventRule().isEmpty)
      AlarmRuleManager.createDummyEventRule()

    val ruleList = AlarmRuleManager.getEventRule()
    val filter = ruleList.foldLeft("")((result, r) => result + " OR " + r.filter()).substring(3)
    println(filter)

    df
      .where(filter)
      .writeStream
      .format("console")
      .start()
  }

}

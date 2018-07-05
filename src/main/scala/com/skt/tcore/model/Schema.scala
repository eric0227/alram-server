package com.skt.tcore.model

import org.apache.spark.sql.types._

object Schema {

  val metricSchema = new StructType()
      .add("nodegroup", StringType, true)
      .add("resource", StringType, true)
      .add("metric", MapType(StringType, DoubleType, true))


  val alarmSchema = new StructType()
      .add("alarmType", StringType, true)
      .add("ruleId", StringType, true)
      .add("detect", BooleanType, true)
      .add("occurCount", IntegerType, true)
      .add("occurTimestamp", TimestampType, true)
      .add("payload", StringType, true)

}

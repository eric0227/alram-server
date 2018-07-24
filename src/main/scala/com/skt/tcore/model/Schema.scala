package com.skt.tcore.model

import org.apache.spark.sql.types._

object Schema {

//  val metricSchema = new StructType()
//      .add("nodegroup", StringType, true)
//      .add("resource", StringType, true)
//      .add("metric", StringType, true)
//      .add("value", DoubleType, true)
//      .add("timestamp", TimestampType, true)
//      //.add("metric", MapType(StringType, DoubleType, true))

  val metricSchema = new StructType()
    .add("name", StringType, true)
    .add("tags", MapType(StringType, StringType, true))
    .add("fields", MapType(StringType, DoubleType, true))
    .add("timestamp", TimestampType, true)


  val alarmSchema = new StructType()
      .add("alarmType", StringType, true)
      .add("ruleId", StringType, true)
      .add("detect", BooleanType, true)
      .add("occurCount", IntegerType, true)
      .add("occurTimestamp", TimestampType, true)
      .add("payload", StringType, true)

}

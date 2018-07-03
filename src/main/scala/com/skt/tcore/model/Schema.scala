package com.skt.tcore.model

import org.apache.spark.sql.types._

object Schema {

  val metricSchema = new StructType()
      .add("nodegroup", StringType, true)
      .add("resource", StringType, true)
      .add("metric", MapType(StringType, DoubleType, true))


  val alarmSchema = new StructType()
      .add("name", StringType, true)
      .add("value", StringType, true)
      .add("operator", StringType, true)
}

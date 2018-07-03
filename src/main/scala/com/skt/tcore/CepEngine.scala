package com.skt.tcore

import com.skt.tcore.model.{MetricFilter, MetricLogic, MetricRule}
import org.apache.spark.sql.{DataFrame, SparkSession}

class CepEngine(spark: SparkSession) {

  case class Query(query: String)

  def createTestQuery() = {
    val df = spark.createDataFrame (
      spark.sparkContext.parallelize(
        Seq(Query("((resource = 'server1' and event = 'cpu' and value > 20.0) OR (resource = 'server1' and event = 'mem' and value > 80.0))"))
      )
    )
    df.createOrReplaceTempView("cep")
  }

  def executeAlarmRule[T](id: String, filter: MetricFilter)(f: DataFrame => T): T = {

    val query = s"""
                   | SELECT resource, event, value
                   | FROM  metricEventLatest
                   | WHERE ${filter.filter()}
                   |
      """.stripMargin
    println(query)

    val df = spark.sql(query)
    f(df)
  }
}

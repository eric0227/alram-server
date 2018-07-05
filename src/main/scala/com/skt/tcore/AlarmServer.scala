package com.skt.tcore

import java.util.concurrent.{Executors, TimeUnit}

import com.skt.tcore.model.{MetricLogic, MetricRule, Schema}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import common.Common._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}


object AlarmServer extends Logging {

  val eventExample =
  """
    {"nodegroup":"g1", "resource":"server1", "metric":{"cpu": 10, "mem": 20, "disk": 30}}
    {"nodegroup":"g1", "resource":"server1", "metric":{"cpu": 20, "mem": 30, "disk": 40}}
    {"nodegroup":"g1", "resource":"server1", "metric":{"cpu": 80, "mem": 85, "disk": 80}}
    {"nodegroup":"g1", "resource":"server1", "metric":{"cpu": 90, "mem": 95, "disk": 95}}
    {"nodegroup":"g1", "resource":"server2", "metric":{"cpu": 15, "mem": 25, "disk": 95}}

    > kafka-console-producer.sh --broker-list localhost:9092 --topic event

  """.stripMargin

  val checkpointPath = "_checkpoint"
  val bootstrap = "192.168.203.105:9092"
  val subscribe = "event"
  val eventDetectTopic = "event-detect"

  def main(args: Array[String]): Unit = {
    val master = Some("local[*]")
    val config = Map[String,String]()//("spark.sql.streaming.checkpointLocation" -> "_checkpoint/AlarmServer"))

    implicit val spark = createSparkSession(master, config)
    AlarmMonitoring.setSparkSession(spark)

    val eventStreamDF = eventKafkaDF(bootstrap, subscribe)
    val logStreamDF = logKafkaDF(bootstrap, subscribe)

    val metricDF = selectMetricEventDF(eventStreamDF)
    //startMetricRollupQuery(metricDF)
    //startMetricStatueQuery(metricDF)
    startEventDetectQuery(metricDF)

    //AlarmMonitoring().startConsoleView()

    spark.streams.awaitAnyTermination()
  }

  def createSparkSession(master: Option[String], config: Map[String, String]): SparkSession = {

    val builder = SparkSession.builder.appName("AlarmServer")
    config.foreach(kv => builder.config(kv._1, kv._2))
    master.foreach(mst => builder.master(mst))
    builder.getOrCreate()
  }

  def eventKafkaDF(bootstrap: String, subscribe: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", subscribe)
      .option("startingOffsets", "latest") // earliest, latest
      .load()
      .selectExpr("timestamp", "CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(Timestamp, String, String)]
      .toDF("timestamp", "key", "value")
  }

  def logKafkaDF(bootstrap: String, subscribe: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", subscribe)
      .option("startingOffsets", "latest") // earliest, latest
      .load()
      .selectExpr("timestamp", "CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(Timestamp, String, String)]
      .toDF("timestamp", "key", "value")
  }

  def selectMetricEventDF(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    val metric = df.select($"timestamp", from_json($"value", schema = Schema.metricSchema).as("data"))
      .filter($"data.metric".isNotNull)
      .select("timestamp", "data.nodegroup", "data.resource", "data.metric")
    metric.printSchema()
    metric.createOrReplaceTempView("metric")

    if(log.isDebugEnabled)
      printConsole(metric)

    metric
  }

  def startMetricStatueQuery(df: DataFrame): StreamingQuery = {
    import df.sparkSession.implicits._

    df
      .groupBy($"nodegroup", $"resource")
      .agg(last($"metric").as("metric"), last("timestamp").as("timestamp"))
      .writeStream
      .option("checkpointLocation", checkpointPath+"/metric_state")
      .outputMode(OutputMode.Complete())
      .format("memory")
      .queryName("metricLatest").start()
  }

  def startMetricRollupQuery(df: DataFrame): StreamingQuery = {
    import df.sparkSession.implicits._

    df
      .select($"timestamp", $"nodegroup", $"resource", explode($"metric"))
      .withWatermark("timestamp", "1 minutes")
      .groupBy( window($"timestamp", "1 minutes"), $"resource", $"key")
      .agg(
         count($"value").as("cnt"), mean($"value").as("mean")
        ,min($"value").as("min"), max($"value").as("max")
        ,stddev($"value").as("stddev")
      )
      //.withColumn("map", map($"key", $"value"))
      //.writeStream
      //.outputMode(OutputMode.Complete())
      //.format("console").option("header", "true").option("truncate", false).start()
      .writeStream
      .option("checkpointLocation", checkpointPath+"/metric_rollup")
      .outputMode(OutputMode.Complete())
      .format("memory")
      .queryName("metricState")
      .start()
  }

  def startEventDetectSinkQuery(df: DataFrame): StreamingQuery = {
    df.writeStream
      .option("kafka.bootstrap.servers", bootstrap)
      .option("topic", eventDetectTopic)
      .option("checkpointLocation", checkpointPath+"/event_detect")
      .outputMode(OutputMode.Append())
      .format("sink.EventDetectSinkProvider")
      .queryName("eventDetect")
      .start()
  }

  def startEventDetectQuery(df: DataFrame): StreamingQuery = {
    import df.sparkSession.implicits._

    // test
    if(AlarmRuleManager.getEventRule().isEmpty)
      AlarmRuleManager.createDummyEventRule()

    val metricDf = df.select($"timestamp", $"nodegroup", $"resource", explode($"metric"))
    metricDf.printSchema()

    val ruleList = AlarmRuleManager.getEventRule()
    val ruleDf = ruleList.toDF()
    ruleDf.printSchema()


    val filter = ruleList.foldLeft("")((result, r) => result + " OR " + r.keyValueFilter()).substring(3)
    println(filter)

    metricDf.as("metric")
      .where(filter)
      .join(ruleDf.as("rule"),
        expr(
          """
            metric.resource = rule.resource AND  metric.key = rule.name
          """)
      )
      .select("rule.ruleId","metric.*")
      .writeStream
      .format("console")
      .start()
  }
}

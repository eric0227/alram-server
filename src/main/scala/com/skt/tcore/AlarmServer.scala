package com.skt.tcore

import java.util.concurrent.{Executors, TimeUnit}

import com.skt.tcore.common.Common
import com.skt.tcore.model.{Alarm, MetricLogic, MetricRule, Schema}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import common.Common._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

import scala.concurrent.duration._

object AlarmServer extends Logging {
  val eventExample =
  """
    {"nodegroup":"g1", "resource":"server1", "metric":"cpu", "value" : 95}
    {"nodegroup":"g1", "resource":"server1", "metric":"mem", "value" : 85}
    {"nodegroup":"g1", "resource":"server1", "metric":"disk", "value" : 75}
    {"nodegroup":"g1", "resource":"server2", "metric":"cpu", "value" : 80}
    {"nodegroup":"g1", "resource":"server2", "metric":"mem", "value" : 85}
    {"nodegroup":"g1", "resource":"server2", "metric":"disk", "value" : 75}

    > kafka-console-producer.sh --broker-list localhost:9092 --topic event

  """.stripMargin

  def main(args: Array[String]): Unit = {
    val master = Some("local[*]")
    val config = Map[String,String]()//("spark.sql.streaming.checkpointLocation" -> "_checkpoint/AlarmServer"))

    implicit val spark = createSparkSession(master, config)
    AlarmMonitoring.setSparkSession(spark)

    val eventStreamDF = readKafkaDF(kafkaServers, metricTopic)
    val metricDF = selectMetricEventDF(eventStreamDF)
    metricEventDetectDF(metricDF)
      .writeStream.format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers).option("topic", alarmTopic)
      .option("checkpointLocation", checkpointPath+"/event_detect_query")
      .start()

    val alarmStreamDF = readKafkaDF(kafkaServers, alarmTopic)
    val alarmDF = selectAlarmDF(alarmStreamDF)

    startMetricStateViewQuery(metricDF)
    startMetricRollupViewQuery(metricDF)

    //val logStreamDF = readKafkaDF(bootstrap, logTopic)


    //startEventDetectSinkQuery(metricDF)
    startContinuousAlarmDetectSinkQuery(alarmDF)

    AlarmMonitoring().startConsoleView()

    spark.streams.awaitAnyTermination()
  }

  def createSparkSession(master: Option[String], config: Map[String, String]): SparkSession = {

    val builder = SparkSession.builder.appName("AlarmServer")
    config.foreach(kv => builder.config(kv._1, kv._2))
    master.foreach(mst => builder.master(mst))
    builder.getOrCreate()
  }

  def readKafkaDF(bootstrap: String, subscribe: String, options: Map[String, String]=Map())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val reader = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", subscribe)
      .option("startingOffsets", "latest") // earliest, latest
      .options(options)
    reader
      .load()
      .selectExpr("timestamp", "CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(Timestamp, String, String)]
      .toDF("timestamp", "key", "value")
  }

  def selectMetricEventDF(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    val metric = df.select($"timestamp", from_json($"value", schema = Schema.metricSchema).as("data"))
      .filter($"data.name".isNotNull)
      .withColumn("timestamp", when($"data.timestamp".isNotNull, $"data.timestamp").otherwise($"timestamp"))
      .withColumn("resource", $"data.tags.host")
      .select($"timestamp", $"resource", $"data.name", explode($"data.fields").as(Seq("metric","value")))
      .withColumn("metric", concat($"name", lit("."), $"metric" ))

    metric.createOrReplaceTempView("metric")

    if(log.isInfoEnabled) metric.printSchema()
    if(log.isDebugEnabled) printConsole(metric)
    metric
  }

  def selectAlarmDF(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    val alarm = df.select($"timestamp", from_json($"value", schema = Schema.alarmSchema).as("data"))
      .filter($"data.alarmType".isNotNull)
      .select("timestamp", "data.alarmType","data.ruleId","data.detect","data.occurCount", "data.occurTimestamp", "data.payload")
      .repartition($"ruleId")
    alarm.printSchema()
    alarm.createOrReplaceTempView("alarm")

    //if(log.isInfoEnabled)
      printConsole(alarm)
    alarm
  }

  def startMetricStateViewQuery(df: DataFrame): StreamingQuery = {
    import df.sparkSession.implicits._
    df
      .withWatermark("timestamp", "5 seconds")
      .groupBy($"nodegroup", $"resource", $"metric")
      .agg(last($"value").as("value"), last("timestamp").as("timestamp"))
      .where(unix_timestamp(current_timestamp()) - unix_timestamp($"timestamp") <= 5 * 60) // 5분 데이터만 유지
      .writeStream
      .option("checkpointLocation", checkpointPath+"/metric_state_view")
      .outputMode(OutputMode.Complete())
      //.trigger(Trigger.ProcessingTime(20.seconds))
      .format("memory")
      .queryName("metric_state_view").start()
  }

  def startMetricRollupViewQuery(df: DataFrame): StreamingQuery = {
    import df.sparkSession.implicits._
    df
      .select($"timestamp", $"nodegroup", $"resource", $"metric", $"value")
      .withWatermark("timestamp", "70 seconds")
      .groupBy(window($"timestamp", "1 minutes"), $"resource", $"metric")
      .agg(
         count($"value").as("cnt"), mean($"value").as("mean")
        ,min($"value").as("min"), max($"value").as("max")
        ,stddev($"value").as("stddev")
      )
      .where(unix_timestamp(current_timestamp()) - unix_timestamp($"window.start") <= 30 * 60) // 과거 30분 까지만 Summary
      .writeStream
      .option("checkpointLocation", checkpointPath+"/metric_rollup_view")
      .outputMode(OutputMode.Complete())
      .trigger(Trigger.ProcessingTime(60.seconds))
      .format("memory")
      .queryName("metric_rollup_view")
      .start()
  }

  def startEventDetectSinkQuery(df: DataFrame): StreamingQuery = {
    df.writeStream
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("topic", alarmTopic)
      .option("checkpointLocation", checkpointPath+"/event_detect_sink")
      .outputMode(OutputMode.Append())
      .format("sink.EventDetectSinkProvider")
      .queryName("eventDetect")
      .start()
  }


  def metricEventDetectDF(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    // test
    if(AlarmRuleManager.getMetricRule().isEmpty)
      AlarmRuleManager.createDummyRule()

    val metricDf = df.select($"timestamp", $"nodegroup", $"resource", $"metric", $"value" )
    if(log.isWarnEnabled) metricDf.printSchema()

    val ruleList = AlarmRuleManager.getMetricRule()
    val ruleDf = ruleList.toDF()
    if(log.isInfoEnabled) ruleDf.printSchema()

    val keyFilter = ruleList.foldLeft("")((result, r) => result + " OR " + r.metricCondition).substring(3)
    if(log.isInfoEnabled) println(keyFilter)

    val valueFilter = ruleList.foldLeft("")((result, r) => result + " OR " + r.condition).substring(3)
    if(log.isInfoEnabled) println(valueFilter)

    val detectDF = metricDf.as("metric")
      .where(keyFilter)
      .withColumn("detect", expr(s"CASE WHEN ${valueFilter} THEN true ELSE false END"))
      .join(ruleDf.as("rule"), expr("metric.resource = rule.resource AND  metric.metric = rule.metric"))
      .withColumn("occurTimestamp", $"metric.timestamp")
      .withColumn("alarmType", lit("simple"))
      .withColumn("occurCount", expr(s"CASE WHEN detect THEN 1 ELSE 0 END"))
      .withColumn("payload", to_json(struct(struct("metric.*").as("metric"), struct("rule.*").as("rule"))))
      .select(to_json(struct("alarmType","ruleId","detect","occurCount","occurTimestamp","payload")).as("value"))
    // case class Alarm(alarmType: String, ruleId: String, detect: Boolean, occurCount: Int, occurTime: Timestamp, payload: String)

    if(log.isInfoEnabled) detectDF.printSchema()
    if(log.isDebugEnabled)  printConsole(detectDF)

    detectDF
  }

  def startContinuousAlarmDetectSinkQuery(df: DataFrame): StreamingQuery = {
    df.writeStream
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("topic", alarmTopic)
      .option("checkpointLocation", checkpointPath+"/continuous_detect_sink")
      .outputMode(OutputMode.Append())
      .format("sink.ContinuousAlarmDetectSinkProvider")
      .queryName("ContinuousDetect")
      .start()
  }

}

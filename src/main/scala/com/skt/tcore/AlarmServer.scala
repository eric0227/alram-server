package com.skt.tcore

import java.util.concurrent.{Executors, TimeUnit}

import com.skt.tcore.model.{MetricLogic, MetricRule, Schema}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.{DataFrame, SparkSession}
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
  """.stripMargin

  val checkoutPrePath = "_checkout"
  val bootstrap = "192.168.203.105:9092"
  val subscribe = "event"
  val eventDetectTopic = "event-detect"

  def main(args: Array[String]): Unit = {
    val master = Some("local[4]")
    val config = Map[String,String]()//("spark.sql.streaming.checkpointLocation" -> "_checkpoint/AlarmServer"))

    implicit val spark = createSparkSession(master, config)
    val streamingDF = createKafkaDF(bootstrap, subscribe)
    printConsole(streamingDF)

    val metricEventDF = selectMetricEventDF(streamingDF)
    metricEventStateQuery(metricEventDF)
    metricEventLatestQuery(metricEventDF)

    metricEventCepAlarmQuery(metricEventDF)

    displayEventState(spark)

    spark.streams.awaitAnyTermination()
  }

  def createSparkSession(master: Option[String], config: Map[String, String]): SparkSession = {

    val builder = SparkSession.builder.appName("AlarmServer")
    config.foreach(kv => builder.config(kv._1, kv._2))
    master.foreach(mst => builder.master(mst))
    builder.getOrCreate()
  }

  def createKafkaDF(bootstrap: String, subscribe: String)(implicit spark: SparkSession): DataFrame = {
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

    val metricEvent = df.select($"timestamp", from_json($"value", schema = Schema.metricSchema).as("data"))
      .filter($"data.metric".isNotNull)
      .select("timestamp", "data.nodegroup", "data.resource", "data.metric")
    metricEvent.printSchema()
    metricEvent.createOrReplaceTempView("metricEvent")

    if(log.isDebugEnabled)
      printConsole(metricEvent)

    metricEvent
  }

  def metricEventLatestQuery(df: DataFrame): StreamingQuery = {
    import df.sparkSession.implicits._

    df
      .groupBy($"nodegroup", $"resource")
      .agg(last($"metric").as("metric"), last("timestamp").as("timestamp"))
      .writeStream
      .option("checkpointLocation", checkoutPrePath+"/metricEventLatest")
      .outputMode(OutputMode.Complete())
      .format("memory")
      .queryName("metricEventLatest").start()
  }

  def metricEventStateQuery(df: DataFrame): StreamingQuery = {
    import df.sparkSession.implicits._

//    df
//      .select($"timestamp", $"nodegroup", $"resource", explode($"metric"))
//      .writeStream
//      .option("checkpointLocation", checkoutPrePath+"/test")
//      .format("console").option("header", "true").option("truncate", false).start()


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
      .option("checkpointLocation", checkoutPrePath+"/metricEventState")
      .outputMode(OutputMode.Complete())
      .format("memory")
      .queryName("metricEventState")
      .start()
  }

  def metricEventCepAlarmQuery(df: DataFrame): StreamingQuery = {
    df.writeStream
      .option("kafka.bootstrap.servers", bootstrap)
      .option("topic", eventDetectTopic)
      .option("checkpointLocation", checkoutPrePath+"/eventDetect")
      .outputMode(OutputMode.Append())
      .format("sink.EventDetectSinkProvider")
      .queryName("eventDetect")
      .start()
  }

  def displayEventState(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val executor = Executors.newScheduledThreadPool(2)

    executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        System.out.println("###################  metricEventLatest  ##################################")
        spark.sql(s"SELECT nodegroup, resource, timestamp, metric FROM metricEventLatest ORDER BY nodegroup, resource").show(truncate = false)
      }
    }, 0, 20, TimeUnit.SECONDS)

    executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        System.out.println("###################  metricEventState  ##################################")
        spark.sql(s"SELECT * FROM metricEventState").printSchema()

        val metricEventStateSummay = spark
          .sql("SELECT window, resource, key, cnt, mean, min, max, stddev FROM metricEventState")
          .withColumn("summary", struct($"cnt", $"mean", $"min", $"max", $"stddev"))
          .groupBy($"window", $"resource")
          .agg(collect_list(map($"key", $"summary")).as("metric"))
          .select($"window", $"resource", $"metric")
          .orderBy($"window", $"resource")

        println("######## metricEventStateSummay #############")
        metricEventStateSummay.printSchema()
        metricEventStateSummay.show(truncate = false)
      }
    }, 3, 20, TimeUnit.SECONDS)
  }
}

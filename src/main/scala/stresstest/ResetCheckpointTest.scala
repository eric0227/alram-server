package stresstest

import com.skt.tcore.AlarmServer
import com.skt.tcore.common.Common.{checkpointPath, kafkaServers, maxOffsetsPerTrigger, metricTopic}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.streaming.Trigger

/**
  * ./kafka-console-producer.sh --broker-list localhost:9092 --topic test-metric
  * {"fields":{"used":13130256384,"used_percent":6.482492161616354},"name":"mem","tags":{"host":"tcore-oi-data-2-1"},"timestamp":1529978760}
  * {"fields":{"used":13130256384,"used_percent":6.482492161616354},"name":"mem","tags":{"host":"tcore-oi-data-2-1"},"timestamp":1529979760}
  *
  *
  */
object ResetCheckpointTest extends App {

  val master = if (args.length >= 1) Some(args(0)) else None
  val builder = SparkSession.builder().appName("ResetCheckpointTest")
  master.foreach(mst => builder.master(mst))
  implicit val spark = builder.getOrCreate()
  println("start application..")

  val options = scala.collection.mutable.HashMap[String, String]()
  maxOffsetsPerTrigger.foreach(max => options += ("maxOffsetsPerTrigger" -> max.toString))

  println(options)

  def readKafkaDF(bootstrap: String, subscribe: String, options: Map[String, String]=Map())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val reader = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", subscribe)
      .option("startingOffsets", "latest")
      .options(options)
    reader
      .load()
      .selectExpr("timestamp", "CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(Timestamp, String, String)]
      .toDF("timestamp", "key", "value")
  }

  val eventStreamDF = readKafkaDF(kafkaServers, metricTopic, options.toMap)(spark)
  eventStreamDF.printSchema()
  val streamDf = AlarmServer.selectMetricEventDF(eventStreamDF)
  streamDf.printSchema()

  val query = streamDf
    .writeStream.format("console")
    .queryName("checkpoint_test")
    .option("checkpointLocation", checkpointPath + "/checkpoint_test")
    .trigger(Trigger.ProcessingTime(0))
    .start()

//  spark.streams.addListener(
//    new OffsetWriteQueryListener(queryName = "checkpoint_test"))

  spark.streams.awaitAnyTermination()
}

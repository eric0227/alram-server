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

  val checkpoint = "checkpoint_test"
  val options = scala.collection.mutable.HashMap[String, String]()

  val master = if (args.length >= 1) Some(args(0)) else None
  val offsetOpt = if (args.length >= 2) Some(args(1)) else None

  val startingOffsets =  offsetOpt.map(_offset => {
    val startingOffsets = if(_offset.forall(Character.isDigit)) {
      val opt = KafkaOffsetManager.getOffset(checkpoint, _offset.toLong)
      if(opt.isEmpty) {
        new IllegalArgumentException("can't reset offset :" + args(1))
      }
      opt.get
    } else _offset
    KafkaOffsetManager.cleanOffset(checkpoint)
    startingOffsets
  }).getOrElse("latest")

  options += ("startingOffsets" -> startingOffsets)
  maxOffsetsPerTrigger.foreach(max => options += ("maxOffsetsPerTrigger" -> max.toString))
  println(options)

  val builder = SparkSession.builder().appName("ResetCheckpointTest")
  master.foreach(mst => builder.master(mst))
  implicit val spark = builder.getOrCreate()

  println("start application..")

  def readKafkaDF(bootstrap: String, subscribe: String, options: Map[String, String]=Map())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val reader = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", subscribe)
      //.option("startingOffsets", "latest")
      //.option("startingOffsets", """{"test-metric":{"0":13004519}}""")
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
    .option("checkpointLocation", checkpointPath + "/" + checkpoint)
    .option("numRows", Int.MaxValue)
    .trigger(Trigger.ProcessingTime(0))
    .start()

  spark.streams.addListener(
    new OffsetWriteQueryListener(queryName = "checkpoint_test"))

  spark.streams.awaitAnyTermination()
}

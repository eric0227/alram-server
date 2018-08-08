package stresstest

import java.text.SimpleDateFormat

import com.skt.tcore.AlarmServer
import com.skt.tcore.common.Common
import com.skt.tcore.common.Common.{checkpointPath, kafkaServers, maxOffsetsPerTrigger, metricTopic}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.streaming.Trigger

import scala.util.Try

/**
  * ./kafka-console-producer.sh --broker-list localhost:9092 --topic test-metric
  * {"fields":{"used":13130256384,"used_percent":6.482492161616354},"name":"mem","tags":{"host":"tcore-oi-data-2-1"},"timestamp":1529978760}
  * {"fields":{"used":13130256384,"used_percent":6.482492161616354},"name":"mem","tags":{"host":"tcore-oi-data-2-1"},"timestamp":1529979760}
  *
  *
  */
object ResetCheckpointTest extends App {

  val queryName = "checkpoint_test"

  def printUsage(): Unit = {
    println("usage: ResetCheckpointTest [earliest | latest | offsets | timestamp | date] ")

    println("examples")
    println("=====================")
    println(""" offsets   : {"topic1":{"0":23,"1":25},"topic2":{"0":10}}""" )
    println(""" timestamp : 1533528363990""" )
    println(""" date      : 2018-08-10 10:05:21""" )
    println()
    println("backup offsets")
    println("=====================")
    KafkaOffsetManager
      .getBackupOffset(queryName)
      .foreach(println)
  }

  println("# test query name : " + queryName)
  println("# backup offset path : " + KafkaOffsetManager.getBackupOffsetPath(queryName))
  println("# spark checkpoint path  : " + KafkaOffsetManager.getCheckpointPath(queryName))
  println()

  if (args.length != 1) {
    printUsage()
    sys.exit()
  }

  val options = scala.collection.mutable.HashMap[String, String]()

  val master = if(System.getProperty("MASTER") != null) Some(System.getProperty("MASTER")) else None

  val startingOffsetsOpt = args(0) match {
    case offset if offset == "earliest" || offset == "latest" => Some(offset)
    case offset if isJson(offset) =>  Some(offset)
    case offset if isTimestamp(offset) => KafkaOffsetManager.getOffset(queryName, offset.toLong)
    case offset if isDate(offset) => {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val timestamp = dateFormat.parse(offset).getTime
      KafkaOffsetManager.getOffset(queryName, timestamp.toLong)
    }
    case _ => None
  }

  def isJson(str: String) = str.contains(":") && str.contains("{") && str.contains("}")
  def isTimestamp(str: String) = Try(str.toLong).map(_ => true).getOrElse(false)
  def isDate(str: String) = Try {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val timestamp = dateFormat.parse(str).getTime
    timestamp
  }.map(_ => true).getOrElse(false)

  if(startingOffsetsOpt.isEmpty) {
    printUsage()
    sys.exit()
  } else {
    //KafkaOffsetManager.deleteBackupOffsetFiles(queryName)
    KafkaOffsetManager.deleteOffsetFiles(queryName)
    options += ("startingOffsets" -> startingOffsetsOpt.get)
  }

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

  spark.streams.addListener(
    new OffsetWriteQueryListener(queryName = "checkpoint_test"))

  val query = streamDf
    .writeStream.format("console")
    .queryName("checkpoint_test")
    .option("checkpointLocation", checkpointPath + "/" + queryName)
    .option("numRows", Int.MaxValue)
    .trigger(Trigger.ProcessingTime(0))
    .start()

  spark.streams.awaitAnyTermination()
}

package stresstest

import java.text.SimpleDateFormat
import java.util.Date

import com.skt.tcore.AlarmServer
import com.skt.tcore.common.Common.{checkpointPath, kafkaServers, maxOffsetsPerTrigger, metricTopic}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.streaming.OffsetSeqLog
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import stresstest.AlarmDetectionStressRddJoinTest.{options, spark}

object ResetCheckpointTest extends App {

  val master = if (args.length == 1) Some(args(0)) else None
  val builder = SparkSession.builder().appName("ResetCheckpointTest")
  master.foreach(mst => builder.master(mst))
  implicit val spark = builder.getOrCreate()
  println("start application..")

  val options = scala.collection.mutable.HashMap[String, String]()
  maxOffsetsPerTrigger.foreach(max => options += ("maxOffsetsPerTrigger" -> max.toString))

  val eventStreamDF = AlarmServer.readKafkaDF(kafkaServers, metricTopic, options.toMap)(spark)
  eventStreamDF.printSchema()
  val streamDf = AlarmServer.selectMetricEventDF(eventStreamDF)
  streamDf.printSchema()

  val query = streamDf
    .writeStream.format("console")
    .queryName("checkpoint_test")
    .option("checkpointLocation", checkpointPath + "/checkpoint_test")
    .trigger(Trigger.ProcessingTime(0))
    .start()

  spark.streams.addListener(new OffsetWriteQueryListener(queryName = "checkpoint_test", topic = metricTopic, "_topics/"+metricTopic+"/offset.out"))

  spark.streams.awaitAnyTermination()
}

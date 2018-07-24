package stresstest

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.skt.tcore.AlarmServer
import com.skt.tcore.common.{Common, RedisClient}
import com.skt.tcore.common.Common.{checkpointPath, kafkaServers, maxOffsetsPerTrigger, metricTopic}
import io.lettuce.core.pubsub.RedisPubSubAdapter
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.streaming.Trigger
import stresstest.AlarmRuleRedisLoaderBroadcast.spark

import scala.collection.JavaConversions._

object AlarmDetectionStressBroadcastUDFTest extends App {

  val master = if (args.length == 1) Some(args(0)) else None
  val builder = SparkSession.builder().appName("spark test")
  master.foreach(mst => builder.master(mst))
  implicit val spark = builder.getOrCreate()
  import spark.implicits._

  val options = scala.collection.mutable.HashMap[String, String]()
  maxOffsetsPerTrigger.foreach(max => options += ("maxOffsetsPerTrigger" -> max.toString))

  var alarmRuleBc: Broadcast[List[MetricRule]] = _
  def createBroadcast(ruleList: List[MetricRule]) = {
    val backup = alarmRuleBc
    alarmRuleBc = spark.sparkContext.broadcast(ruleList)
    if(backup != null) backup.destroy()
  }

  AlarmRuleRedisLoader { list =>
    println(list.size)
    Common.watchTime("create Broadcast") {
      createBroadcast(list.toList)
    }
  }.loadRedisRule()

  val eventStreamDF = AlarmServer.readKafkaDF(kafkaServers, metricTopic, options.toMap)(spark)
  eventStreamDF.printSchema()
  val streamDf = AlarmServer.selectMetricEventDF(eventStreamDF)
  streamDf.printSchema()

  val userFilter = (resource: String, metric: String, value: Double) => {
    val ruleList = alarmRuleBc.value
    ruleList.exists(r => resource == r.resource && metric == r.metric && r.eval(value))
  }

  def dynamicFilter = udf(userFilter)
  streamDf
    .filter(dynamicFilter($"resource", $"metric", $"value") === true)
    .mapPartitions { iter => List(iter.length).iterator }
    .writeStream
    .format("stresstest.CountSinkProvider")
    //.format("console")
    //.option("header", "true").option("truncate", false).option("numRows", Int.MaxValue)
    .trigger(Trigger.ProcessingTime(0))
    .option("checkpointLocation", checkpointPath+"/udf")
    .start()

  spark.streams.awaitAnyTermination()
}

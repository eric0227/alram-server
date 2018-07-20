package test

import java.util.Date

import com.skt.tcore.AlarmServer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import com.skt.tcore.common.Common._

object AlarmDetectionStressBroadcastUDFTest extends App {

  val master = if (args.length == 1) Some(args(0)) else None
  val builder = SparkSession.builder().appName("spark test")
  master.foreach(mst => builder.master(mst))
  implicit val spark = builder.getOrCreate()
  import spark.implicits._

  var alarmRuleBc: Broadcast[List[MetricRule]] = null
  def createOrReplaceMetricRule(ruleList: List[MetricRule]) = {
    val backup = alarmRuleBc
    alarmRuleBc = spark.sparkContext.broadcast(ruleList)
    if(backup != null) backup.destroy()
  }

  val r = scala.util.Random
  createOrReplaceMetricRule {
    (1 to 1000).map { i => MetricRule("server" + i, "cpu", 0, ">") }.toList
  }

  val options = scala.collection.mutable.HashMap[String, String]()
  maxOffsetsPerTrigger.foreach(max => options += ("maxOffsetsPerTrigger" -> max.toString))

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
    .format("test.CountSinkProvider")
    //.format("console")
    //.option("header", "true").option("truncate", false).option("numRows", Int.MaxValue)
    .trigger(Trigger.ProcessingTime(0))
    .option("checkpointLocation", checkpointPath+"/udf")
    .start()

  spark.streams.awaitAnyTermination()
}


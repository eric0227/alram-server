package stresstest

import com.skt.tcore.AlarmServer
import com.skt.tcore.common.Common.{checkpointPath, kafkaServers, maxOffsetsPerTrigger, metricTopic}
import com.skt.tcore.common.{Common, RedisClient}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.streaming.Trigger

object AlarmDetectionStressRedisUDFTest extends App {

  val master = if (args.length == 1) Some(args(0)) else None
  val builder = SparkSession.builder().appName("spark test")
  master.foreach(mst => builder.master(mst))
  implicit val spark = builder.getOrCreate()
  import spark.implicits._

  val options = scala.collection.mutable.HashMap[String, String]()
  maxOffsetsPerTrigger.foreach(max => options += ("maxOffsetsPerTrigger" -> max.toString))

  val eventStreamDF = AlarmServer.readKafkaDF(kafkaServers, metricTopic, options.toMap)(spark)
  eventStreamDF.printSchema()
  val streamDf = AlarmServer.selectMetricEventDF(eventStreamDF)
  streamDf.printSchema()

  val userFilter = (resource: String, metric: String, value: Double) => {
    val redis = RedisClient.getInstance().redis
    val opValue = redis.hget(Common.metricRuleKey, resource+":"+metric)

    if(opValue == null) false
    else {
      val Array(op, ruleValue) = opValue.split(":")
      val rule = MetricRule(resource, metric, ruleValue.toDouble, op)
      rule.eval(value)
    }
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
    .option("checkpointLocation", checkpointPath+"/redis")
    .start()

  spark.streams.awaitAnyTermination()
}

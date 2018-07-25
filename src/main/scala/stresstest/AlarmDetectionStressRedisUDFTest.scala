package stresstest

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.skt.tcore.AlarmServer
import com.skt.tcore.common.Common.{checkpointPath, kafkaServers, maxOffsetsPerTrigger, metricTopic}
import com.skt.tcore.common.{Common, RedisClient}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.streaming.Trigger
import stresstest.AlarmDetectionStressBroadcastUDFTest.userFilter

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

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  val userFilter = (resource: String, metric: String, value: Double) => {
    val redis = RedisClient.getInstance().redis
    val key = Common.metricRuleKey + ":" + resource
    val json = redis.hget(key, metric)

    if (json == null) false
    else {
      val rule = mapper.readValue[MetricRule](json, classOf[MetricRule])
      rule.eval(value)
    }
  }

  import spark.implicits._
  def dynamicFilter = udf(userFilter)
  streamDf
    //.filter(dynamicFilter($"resource", $"metric", $"value") === true)
    //.mapPartitions { iter => List(iter.length).iterator }
    .map { row =>
      val resource = row.getAs[String]("resource")
      val metric = row.getAs[String]("metric")
      val value = row.getAs[Double]("value")
      val chk = if (userFilter(resource, metric, value)) 1 else 0
      (metric, chk, 1)
    }
    .mapPartitions { iter =>
      iter.toList.groupBy(d => (d._1,d._2)).map(d => (d._1._1, d._1._2, d._2.size)).iterator
    }
    .writeStream
    .format("stresstest.CountSinkProvider")
    //.format("console")
    //.option("header", "true").option("truncate", false).option("numRows", Int.MaxValue)
    .trigger(Trigger.ProcessingTime(0))
    .option("checkpointLocation", checkpointPath+"/redis")
    .start()

  spark.streams.awaitAnyTermination()
}

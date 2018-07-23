package stresstest

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.skt.tcore.AlarmServer
import com.skt.tcore.common.Common.{checkpointPath, kafkaServers, maxOffsetsPerTrigger, metricTopic}
import com.skt.tcore.common.{Common, RedisClient}
import io.lettuce.core.RedisConnectionStateAdapter
import io.lettuce.core.pubsub.RedisPubSubAdapter
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import stresstest.AlarmDetectionStressBroadcastUDFTest.alarmRuleBc
import stresstest.AlarmDetectionStressRedisUDFTest.{args, options, spark}

import scala.collection.JavaConversions._

object AlarmDetectionStressRddJoinTest extends App {

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

  val redisConn = RedisClient.getInstance().client.connectPubSub()
  redisConn.addListener(new RedisPubSubAdapter[String, String] {
    override def message(channel: String, message: String): Unit = {
      super.message(channel, message)
      println("message :: " + message)
      loadRedisRule()
    }
  })
  val redisCmd = redisConn.sync()
  redisCmd.subscribe(Common.metricRuleSyncChannel)

  def loadRedisRule(): Unit = {
    val redis = RedisClient.getInstance().redis
    val list = redis.hgetall(Common.metricRuleKey).values().toList
    println("load rule :: " + list.size)

    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    val ruleList = list.map { json =>
      mapper.readValue[MetricRule](json, classOf[MetricRule])
    }
    createRuleDF(ruleList)
  }
  loadRedisRule()

  var ruleDf: DataFrame = _
  def createRuleDF(ruleList: List[MetricRule])= {
    val df: DataFrame = spark.sqlContext.createDataFrame(ruleList)
    df.repartition(df("resource")).cache().createOrReplaceTempView("metric_rule")
    if(ruleDf != null) ruleDf.unpersist()
    ruleDf = df
    spark.sql("select * from metric_rule").show(truncate = false)
  }

  val join = spark.sql(
    """
      | select m.timestamp, m.resource, m.metric,
      |        case when r.op = '='  and m.value =  r.value then 1
      |             when r.op = '>'  and m.value >  r.value then 1
      |             when r.op = '>=' and m.value >= r.value then 1
      |             when r.op = '<'  and m.value <  r.value then 1
      |             when r.op = '<=' and m.value <= r.value then 1
      |             when r.op = '!=' and m.value != r.value then 1
      |             when r.op = '<>' and m.value <> r.value then 1
      |        else 0 end chk
      | from metric m
      | inner join metric_rule r
      | on m.resource = r.resource and m.metric = r.metric
    """.stripMargin)
      .mapPartitions { iter => List(iter.length).iterator }

  join.writeStream
    .format("stresstest.CountSinkProvider")
    //.format("console")
    //.option("header", "true").option("truncate", false).option("numRows", Int.MaxValue)
    .trigger(Trigger.ProcessingTime(0))
    .option("checkpointLocation", checkpointPath+"/spark-streaming")
    .start()

  spark.streams.awaitAnyTermination()
}

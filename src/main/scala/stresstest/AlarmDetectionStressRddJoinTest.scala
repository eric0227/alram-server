package stresstest

import com.skt.tcore.AlarmServer
import com.skt.tcore.common.Common
import com.skt.tcore.common.Common.{checkpointPath, kafkaServers, maxOffsetsPerTrigger, metricTopic}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

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

  def createRuleDF(ruleList: List[MetricRule]) = synchronized {
    val df: DataFrame = spark.sqlContext.createDataFrame(ruleList)
    df.repartition(df("resource")).cache().createOrReplaceTempView("metric_rule")
    if (ruleDf != null) ruleDf.unpersist()
    ruleDf = df
    spark.sql("select * from metric_rule").show(truncate = false)
  }

  var ruleDf: DataFrame = _
  AlarmRuleRedisLoader { list =>
    println(list.size)
    Common.watchTime("create Rule RDD") {
      createRuleDF(list.toList)
    }
  }.loadRedisRule()

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
    .mapPartitions { iter =>
      List(iter.length).iterator
    }

  join.writeStream
    .format("stresstest.CountSinkProvider")
    //.format("console")
    //.option("header", "true").option("truncate", false).option("numRows", Int.MaxValue)
    .trigger(Trigger.ProcessingTime(0))
    .option("checkpointLocation", checkpointPath + "/spark-streaming")
    .start()

  spark.streams.awaitAnyTermination()
}

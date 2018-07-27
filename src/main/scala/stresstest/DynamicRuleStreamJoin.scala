package stresstest

import com.skt.tcore.AlarmServer
import com.skt.tcore.common.Common.{checkpointPath, kafkaServers, maxOffsetsPerTrigger, metricTopic}
import com.skt.tcore.model.Schema
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import stresstest.AlarmDetectionStressRddJoinTest.args


object DynamicRuleStreamJoin extends App {

  """
     ./kafka-console-producer.sh --broker-list localhost:9092  --topic dynamic_rule
     {"id":"1", "resource":"tcore-oi-data-2-1", "metric":"mem.used_percent", "op":">", "value":50}
     {"id":"2", "resource":"tcore-oi-data-2-2", "metric":"mem.used_percent", "op":">", "value":60}
     {"id":"3", "resource":"tcore-oi-data-2-3", "metric":"mem.used_percent", "op":">", "value":70}

     {"id":"1", "resource":"tcore-oi-data-2-1", "metric":"mem.used_percent", "op":">", "value":80}
     {"id":"2", "resource":"tcore-oi-data-2-2", "metric":"mem.used_percent", "op":">", "value":90}
     {"id":"3", "resource":"tcore-oi-data-2-3", "metric":"mem.used_percent", "op":">", "value":100}

     {"id":"2000", "resource":"tcore-oi-data-2-2000", "metric":"mem.used_percent", "op":">", "value":100}

  """.stripMargin

  val master = if (args.length == 1) Some(args(0)) else None
  val builder = SparkSession.builder().appName("AlarmDetectionStressRddJoinTest")
  master.foreach(mst => builder.master(mst))
  builder.config("spark.sql.streaming.checkpointLocation",  checkpointPath+"/SparkSessionTest")

  implicit val spark = builder.getOrCreate()

  import spark.implicits._

  val options = scala.collection.mutable.HashMap[String, String]()
  maxOffsetsPerTrigger.foreach(max => options += ("maxOffsetsPerTrigger" -> max.toString))

  val dynamicRuleDF = AlarmServer.readKafkaDF(kafkaServers, "dynamic_rule", options.toMap)(spark)
  AlarmServer.startDynamicRule(dynamicRuleDF)

  val eventStreamDF = AlarmServer.readKafkaDF(kafkaServers, metricTopic, options.toMap)(spark)
  eventStreamDF.printSchema()
  val streamDf = AlarmServer.selectMetricEventDF(eventStreamDF)
  streamDf.printSchema()

  println("start application..")

  var ruleDf: DataFrame = _
  def createRuleDF(ruleList: List[MetricRule]) = synchronized {
    if(ruleDf != null) ruleDf.unpersist(true)
    ruleDf = broadcast(spark.sqlContext.createDataFrame(ruleList)).toDF().cache()
    ruleDf.createOrReplaceTempView("metric_rule")
    startQuery()
    println("create dataframe ..ok")
  }

  AlarmRuleRedisLoader { list =>
    createRuleDF(list.toList)
  }.loadRedisRule()

  var query: StreamingQuery = _
  def startQuery() = synchronized {
    if(query != null) query.stop()

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
        | inner join (
        |   select resource, metric, op, value from metric_rule
        |   union all
        |   select resource, metric, op, value from dynamic_rule
        | ) r
        | on m.resource = r.resource and m.metric = r.metric
      """.stripMargin)
      .mapPartitions { iter =>
        iter.toList.map { r =>
          (r.getAs[String]("metric"), r.getAs[Int]("chk"), 1)
        }.groupBy(d => (d._1,d._2)).map(d => (d._1._1, d._1._2, d._2.size)).iterator
      }
    query = join.writeStream
      .format("stresstest.CountSinkProvider")
      .trigger(Trigger.ProcessingTime(0))
      .option("checkpointLocation", checkpointPath + "/rdd")
      .start()
  }
  //startQuery()

  while(true) {
    spark.streams.awaitAnyTermination()
    Thread.sleep(1000)
  }
}

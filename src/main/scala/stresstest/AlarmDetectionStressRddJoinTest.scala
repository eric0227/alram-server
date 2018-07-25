package stresstest

import com.skt.tcore.AlarmServer
import com.skt.tcore.common.Common
import com.skt.tcore.common.Common.{checkpointPath, kafkaServers, maxOffsetsPerTrigger, metricTopic}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import stresstest.AlarmRuleRedisLoaderRDD.spark

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
    spark.sql("select metric, count(*) from metric_rule group by metric").show(truncate = false)
  }

  var ruleDf: DataFrame = _
  AlarmRuleRedisLoader { list =>
    createRuleDF(list.toList)
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
      iter.toList.map { r =>
        (r.getAs[String]("metric"), r.getAs[Int]("chk"), 1)
      }.groupBy(d => (d._1,d._2)).map(d => (d._1._1, d._1._2, d._2.size)).iterator
      //List(iter.length).iterator{"fields":{"active":13999108096,"available":188894736384,"available_percent":93.25854820870096,"buffered":707768320,"cached":3480580096,"free":185230909440,"inactive":1324851200,"slab":469602304,"total":202549514240,"used":13130256384,"used_percent":6.482492161616354,"wired":0},"name":"mem","tags":{"host":"tcore-oi-data-2-1"},"timestamp":1529978760}
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

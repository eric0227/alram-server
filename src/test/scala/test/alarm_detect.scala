package test

import java.util.Date

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object alarm_detect extends App {

  val data =
  """
    | {"nodegroup":"g1","resource": "r1","event":"sys.cpu.usage", "metric":90, "log":""}
    | {"nodegroup":"g1","resource": "r1","event":"sys.cpu.usage", "metric":10, "log":""}
    | {"nodegroup":"g1","resource": "r1","event":"sys.memory.usage", "metric":90, "log":""}
    | {"nodegroup":"g1","resource": "r1","event":"sys.disk.usage", "metric":90, "log":""}
    |
  """

  import org.apache.spark.sql.SparkSession
  val ss = SparkSession
    .builder()
    .appName("AlertStructuredStreaming")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", 10)
    .getOrCreate()

  import ss.implicits._

  val kafkaDf = ss
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", args(0))
    .option("subscribe", "cep-streaming")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()

  val schema = StructType(Seq(
    StructField("nodegroup", StringType, true),
    StructField("metric", DoubleType, true),
    StructField("resource", StringType, true),
    StructField("event", StringType, true),
    StructField("log", StringType, true)
  ))

  case class AlarmRule(id: String, event: String, metric: Double, op: String)
  var alarmRuleDf: DataFrame = null
  def createOrReplaceAlarmRule(ruleList: List[AlarmRule]) = {
    val df = ruleList.toDF
    if (alarmRuleDf != null) alarmRuleDf.unpersist()
    alarmRuleDf = df
    alarmRuleDf.cache()
    alarmRuleDf.createOrReplaceTempView("rule")
    alarmRuleDf.show()
  }
  createOrReplaceAlarmRule(List(AlarmRule("1", "sys.cpu.usage", 80, ">"), AlarmRule("2", "sys.memory.usage", 90, ">"), AlarmRule("3", "sys.disk.usage", 90, ">")))

  val selectQuery = kafkaDf
    .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")
    .as[(String, String)]
    .select(from_json($"value", schema = schema).as("data"))
    .selectExpr(
      "data.resource as resource", "data.nodegroup as nodegroup", "data.event as event",
      "CAST(data.metric AS DOUBLE) as metric",
      "data.log as log").select("nodegroup", "event", "metric", "log")
  selectQuery.createOrReplaceTempView("metric")
  // RDD Operation
//  selectQuery.mapPartitions { iter =>
//    alarmRuleDf.collect()
//    iter.filter { r =>
//      true
//    }
//  }.createOrReplaceTempView("metric")


  val join = ss.sql(
    """
      | select rule.id, metric.nodegroup, metric.event, metric.metric, metric.log,
      |        case when rule.op = '=' and metric.metric = rule.metric then 1
      |             when rule.op = '>' and metric.metric > rule.metric then 1
      |             when rule.op = '<' and metric.metric < rule.metric then 1
      |        else 0 end chk
      | from metric
      | inner join rule
      | on metric.event = rule.event
    """.stripMargin)
  //.filter($"chk" === 1)

  join.writeStream.format("console").option("truncate", false).start()
  val finalQuery = join.select(to_json(struct($"rule.id",$"nodegroup", $"event", $"log", $"metric")).as("value"))
//  val executingQuery = finalQuery
//    .writeStream
//    .format("kafka")
//    .option("kafka.bootstrap.servers", args(0))
//    .option("topic", "event-streaming")
//    .option("checkpointLocation", "/tmp/spark-streaming")
//    .outputMode(OutputMode.Append())
//    .start()

  (1 to 1000) foreach { i =>
    val r = scala.util.Random
    createOrReplaceAlarmRule(List(AlarmRule("1", "sys.cpu.usage", r.nextInt(100), ">"), AlarmRule("2", "sys.memory.usage", r.nextInt(100), ">"), AlarmRule("3", "sys.disk.usage", r.nextInt(100), ">")))
    Thread.sleep(100)
    println(new Date(), i)
  }
  ss.streams.awaitAnyTermination()
}

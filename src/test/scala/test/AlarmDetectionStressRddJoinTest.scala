package test

import java.util.Date

import com.skt.tcore.AlarmServer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import com.skt.tcore.common.Common._

case class MetricRule(resource: String, metric: String, value: Double, op: String = ">") {
  def eval(v: Double) : Boolean = op match {
    case "=" =>  v == value
    case ">" =>  v >  value
    case ">=" => v >= value
    case "<" =>  v <  value
    case "<=" => v <= value
    case "!=" | "<>" => v != value
    case _ => false
  }
}

object AlarmDetectionStressRddJoinTest extends App {

  val master = if (args.length == 1) Some(args(0)) else None
  val builder = SparkSession.builder().appName("spark test")
  master.foreach(mst => builder.master(mst))
  implicit val spark = builder.getOrCreate()
  import spark.implicits._

  val eventStreamDF = AlarmServer.readKafkaDF(kafkaServers, eventTopic)

  eventStreamDF.printSchema()
  val streamDf = AlarmServer.selectMetricEventDF(eventStreamDF)
  streamDf.printSchema()

  def createRuleDF()= {
    val r = scala.util.Random
    val ruleList = (1 to 1000) map { i => MetricRule("server" + i, "cpu", r.nextInt(100), ">") }
    val df: DataFrame = spark.sqlContext.createDataFrame(ruleList)
    df.repartition(df("resource")).cache().createOrReplaceTempView("metric_rule")
    spark.sql("select * from metric_rule").show(truncate = false)
  }
  createRuleDF()

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
    .format("test.CountSinkProvider")
    //.format("console")
    //.option("header", "true").option("truncate", false).option("numRows", Int.MaxValue)
    .trigger(Trigger.ProcessingTime(0))
    .option("checkpointLocation", checkpointPath+"/spark-streaming")
    .start()

  spark.streams.awaitAnyTermination()
}


class CountSink(options: Map[String, String]) extends Sink with Logging {
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    println(new Date().toString + "::" + data.collect().map(_.getInt(0)).sum)
  }
}


class CountSinkProvider extends StreamSinkProvider with DataSourceRegister {
  def createSink(
                  sqlContext: SQLContext,
                  parameters: Map[String, String],
                  partitionColumns: Seq[String],
                  outputMode: OutputMode): Sink = {

    new CountSink(parameters)
  }

  def shortName(): String = "countSink"
}

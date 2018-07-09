package com.skt.tcore

import java.util.concurrent.{Executors, TimeUnit}

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.skt.tcore.common.Common._
import com.skt.tcore.model.{MetricRollupRule, MetricRule}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import test.{MetricKafkaProducer, SparkSessionTestWrapper}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.async



class MetricRuleTest extends FunSuite with SparkSessionTestWrapper with DatasetComparer {
  import spark.implicits._
  implicit val sc = spark.sqlContext
  implicit val sp = spark

  val bootstrap = "192.168.203.105:9092"
  val eventTopic = "event"

  val metricSchema = new StructType()
    .add("nodegroup", StringType, true)
    .add("resource", StringType, true)
    .add("metric", StringType, true)
    .add("value", DoubleType, true)
    .add("timestamp", TimestampType, true)

  val metric = Array(
     "{'nodegroup':'g1', 'resource':'server1', 'metric':'cpu', 'value' : 95}"
    ,"{'nodegroup':'g1', 'resource':'server1', 'metric':'mem', 'value' : 85}"
    ,"{'nodegroup':'g1', 'resource':'server1', 'metric':'disk', 'value' : 75}"
    ,"{'nodegroup':'g1', 'resource':'server1', 'metric':'cpu', 'value' : 80}"
    ,"{'nodegroup':'g1', 'resource':'server1', 'metric':'mem', 'value' : 90}"
    ,"{'nodegroup':'g1', 'resource':'server1', 'metric':'disk', 'value' : 95}"
  )


  val eventStreamDF = AlarmServer.readKafkaDF(bootstrap, eventTopic)
  eventStreamDF.printSchema()

  test("metric(simple type)") {
    val metricDF = AlarmServer.selectMetricEventDF(eventStreamDF)
    metricDF.printSchema()
    printConsole(eventStreamDF)

    AlarmRuleManager.clear()
    AlarmRuleManager.addRule(MetricRule("r1", "server1", "cpu", 90, ">"))
    AlarmRuleManager.addRule(MetricRule("r2", "server1", "mem", 50, ">"))
    AlarmRuleManager.addRule(MetricRule("r3", "server1", "disk", 95, ">"))
    AlarmRuleManager.getMetricRule().foreach(r => println(r.filterStr))

    val eventDF =  AlarmServer.metricEventDetectDF(metricDF)
    printConsole(eventDF)
    metric.foreach(d => MetricKafkaProducer.sendKafka(eventTopic, "", d))

    Thread.sleep(1000 * 10)
    //spark.streams.awaitAnyTermination()
  }


  test("metric(pre aggregation)") {
    val metricDF = AlarmServer.selectMetricEventDF(eventStreamDF)
    metricDF.printSchema()
    printConsole(eventStreamDF)

    AlarmServer.startMetricStateViewQuery(metricDF)
    metric.foreach(d => MetricKafkaProducer.sendKafka(eventTopic, "", d))
    Thread.sleep(1000 * 1)

    System.out.println("###################  metric_state_view  #########################")
    spark.sql("SELECT * FROM metric_state_view").show(truncate = false)
    Thread.sleep(1000 * 1)
    spark.sql("SELECT * FROM metric_state_view").show(truncate = false)


    val mr = MetricRule("r1", "server1", "cpu", 80, ">=")
    val v = GetMetricValue.getMetricValue(mr)
    v.foreach {
      case Some(value) =>
        println(value)
        assert(value == 80)
        assert(mr.eval(value))
      case None =>
    }
    Await.result(v, Duration.Inf)

    val metricValueDF = GetMetricValue.getMetricValue(
         List(
            MetricRule("r1", "server1", "cpu", 80, ">=")
           ,MetricRule("r1", "server1", "mem", 90, ">=")
           ,MetricRule("r1", "server1", "disk", 99, ">=")
         )
    )
    metricValueDF.show(truncate = false)
    Thread.sleep(1000 * 3)
    //spark.streams.awaitAnyTermination()
  }
}

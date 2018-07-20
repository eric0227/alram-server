package com.skt.tcore

import java.util.concurrent.{Executors, TimeUnit}

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.skt.tcore.common.Common._
import com.skt.tcore.model.{MetricRule}

import org.scalatest.FunSuite
import test.{SparkSessionTestWrapper}

class MetricSimpleRuleTest extends FunSuite with SparkSessionTestWrapper with DatasetComparer {
  import spark.implicits._

  val eventStreamDF = AlarmServer.readKafkaDF(kafkaServers, eventTopic)
  eventStreamDF.printSchema()

  test("metric(simple type)") {
    val metricDF = AlarmServer.selectMetricEventDF(eventStreamDF)
    metricDF.printSchema()
    //printConsole(eventStreamDF)

    AlarmRuleManager.clear()
    AlarmRuleManager.addRule(MetricRule("r1", "server1", "cpu", 90, ">"))
    AlarmRuleManager.addRule(MetricRule("r2", "server1", "mem", 50, ">"))
    AlarmRuleManager.addRule(MetricRule("r3", "server1", "disk", 95, ">"))
    AlarmRuleManager.getMetricRule().foreach(r => println(r.condition))

    val eventDF =  AlarmServer.metricEventDetectDF(metricDF)
    printConsole(eventDF, Integer.MAX_VALUE)

    startMetricSend()
    Thread.sleep(1000 * 10)
    stopMetricSend()

    spark.streams.active.foreach(_.stop())
  }
}

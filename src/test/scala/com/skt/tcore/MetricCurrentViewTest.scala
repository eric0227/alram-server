package com.skt.tcore

import java.util.concurrent.{Executors, TimeUnit}

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.skt.tcore.common.Common
import com.skt.tcore.common.Common._
import com.skt.tcore.model.{Metric, MetricRule}
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import test.SparkSessionTestWrapper

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import com.skt.tcore.common.Common

class MetricCurrentViewTest extends FunSuite with SparkSessionTestWrapper with DatasetComparer {

  val eventStreamDF = AlarmServer.readKafkaDF(kafkaServers, metricTopic)
  eventStreamDF.printSchema()

  test("get metric value (pre aggregation)") {
    val metricDF = AlarmServer.selectMetricEventDF(eventStreamDF)
    metricDF.printSchema()
    //printConsole(eventStreamDF)
    val stateViewQuery = AlarmServer.startMetricStateViewQuery(metricDF)
    while(!stateViewQuery.isActive) Thread.sleep(100)

    startMetricSend(10000, sleep=100)

    System.out.println("*** metric_state_view")
    spark.sql("SELECT * FROM metric_state_view").show(truncate = false)

    (1 to 100) foreach { i =>
      println("count :: " + i)
      println("metric_state_view totalCount :: "
        + MetricSearchService.selectCurrentMetric("select count(1) as cnt from metric_state_view").head().getAs[Long]("cnt"))
      Common.watchTime("metric_state_view query") {

        val f = for {
          cpuOpt <- MetricSearchService.getCurrentMetricValue(Metric("server1", "cpu"))
          memOpt <- MetricSearchService.getCurrentMetricValue(Metric("server1", "mem"))
        } yield (cpuOpt, memOpt)
        println(Await.result(f, Duration.Inf))
      }
    }
    stopMetricSend()
    spark.streams.active.foreach(_.stop())
  }

  test("select metric (pre aggregation)") {
    val metricDF = AlarmServer.selectMetricEventDF(eventStreamDF)
    metricDF.printSchema()

    val stateViewQuery = AlarmServer.startMetricStateViewQuery(metricDF)
    while(!stateViewQuery.isActive) Thread.sleep(100)

    startMetricSend(1)

    System.out.println("*** metric_state_view")
    spark.sql("SELECT * FROM metric_state_view").show(truncate = false)

    (1 to 100) foreach { i =>
      println("count :: " + i)
      println("metric_state_view totalCount :: "
        + MetricSearchService.selectCurrentMetric("select count(1) as cnt from metric_state_view").head().getAs[Long]("cnt"))

      Common.watchTime("metric_state_view query") {
        MetricSearchService.selectCurrentMetric(List(
          Metric("server1", "cpu"),
          Metric("server1", "mem"),
          Metric("server1", "disk")
        )).show(truncate = false)
      }
    }
    stopMetricSend()
    spark.streams.active.foreach(_.stop())
  }

  test("select metric_state_view (pre aggregation)") {
    val metricDF = AlarmServer.selectMetricEventDF(eventStreamDF)
    metricDF.printSchema()
    val stateViewQuery = AlarmServer.startMetricStateViewQuery(metricDF)

    while(!stateViewQuery.isActive) Thread.sleep(100)

    startMetricSend(10)

    System.out.println("*** metric_state_view")
    Thread.sleep(1000 * 10)
    spark.sql("SELECT * FROM metric_state_view").show(truncate = false)

    (1 to 100) foreach { i =>
      println("count :: " + i)
      println("metric_state_view totalCount :: "
        + MetricSearchService.selectCurrentMetric("select count(1) as cnt from metric_state_view").head().getAs[Long]("cnt"))

      Common.watchTime("metric_state_view query") {
        MetricSearchService.selectCurrentMetricRule(List(
          MetricRule("r1", "server1", "cpu", 70, ">=" ),
          MetricRule("r1", "server1", "mem", 80, ">=" ),
          MetricRule("r1", "server1", "disk", 90, ">=" )
        )).show(truncate = false)
      }
    }
    stopMetricSend()
    spark.streams.active.foreach(_.stop())
  }
}

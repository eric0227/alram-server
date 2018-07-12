package com.skt.tcore

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.skt.tcore.common.Common
import com.skt.tcore.model.{Metric, MetricRollupRule, MetricRule}
import org.scalatest.FunSuite
import test.SparkSessionTestWrapper

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class MetricRollupViewTest extends FunSuite with SparkSessionTestWrapper with DatasetComparer {

  val eventStreamDF = AlarmServer.readKafkaDF(bootstrap, eventTopic)
  eventStreamDF.printSchema()

  test("select metric_rollup_view (pre aggregation)") {
    val metricDF = AlarmServer.selectMetricEventDF(eventStreamDF)
    metricDF.printSchema()
    //printConsole(eventStreamDF)
    val rollupViewQuery = AlarmServer.startMetricRollupViewQuery(metricDF)
    while(!rollupViewQuery.isActive) Thread.sleep(100)

    startMetricSend(10, 10)

    System.out.println("*** metric_rollup_view")
    spark.sql("SELECT * FROM metric_rollup_view").show(truncate = false)

    (1 to 1000) foreach { i =>
      println("count :: " + i)
      println("metric_rollup_view totalCount :: "
        + spark.sql("select count(1) as cnt from metric_rollup_view").head().getAs[Long]("cnt"))

      Common.watchTime("metric_rollup_view query") {
        spark.sql("select * from metric_rollup_view order by window.start desc").show(numRows = 5, truncate = false)
      }
    }
    stopMetricSend()
    spark.streams.active.foreach(_.stop())
  }

  test("get metric_rollup_view value (pre aggregation)") {
    val metricDF = AlarmServer.selectMetricEventDF(eventStreamDF)
    metricDF.printSchema()

    val rollupViewQuery = AlarmServer.startMetricRollupViewQuery(metricDF)
    while(!rollupViewQuery.isActive) Thread.sleep(100)

    startMetricSend(10, 10)

    System.out.println("*** metric_rollup_view")
    spark.sql("SELECT * FROM metric_rollup_view").show(truncate = false)

    (1 to 100) foreach { i =>
      println("count :: " + i)
      println("metric_rollup_view totalCount :: "
        + spark.sql("select count(1) as cnt from metric_rollup_view").head().getAs[Long]("cnt"))

      Common.watchTime("metric_rollup_view query") {
        val f = for {
          cpuOpt <- MetricSearchService.getRollupMetricMean(Metric("server1", "cpu"), 5)
          memOpt <- MetricSearchService.getRollupMetricMean(Metric("server1", "mem"), 5)
        } yield (cpuOpt, memOpt)
        println(Await.result(f, Duration.Inf))
      }
    }
    stopMetricSend()
    spark.streams.active.foreach(_.stop())
  }

  test("select metric_rollup_view with condition (pre aggregation)") {
    val metricDF = AlarmServer.selectMetricEventDF(eventStreamDF)
    metricDF.printSchema()
    val rollupViewQuery = AlarmServer.startMetricRollupViewQuery(metricDF)
    while(!rollupViewQuery.isActive) Thread.sleep(100)

    startMetricSend(10, 10)

    System.out.println("*** metric_rollup_view")
    Thread.sleep(1000 * 10)
    spark.sql("SELECT * FROM metric_rollup_view").show(truncate = false)

    (1 to 100) foreach { i =>
      println("count :: " + i)
      println("metric_rollup_view totalCount :: "
        + MetricSearchService.selectCurrentMetric("select count(1) as cnt from metric_rollup_view").head().getAs[Long]("cnt"))

      Common.watchTime("metric_rollup_view query") {
        MetricSearchService.selectRollupMetricMean(List(
          MetricRollupRule("r1", "server1", "cpu", "mean", 70, ">=" ),
          MetricRollupRule("r1", "server1", "mem", "mean", 80, ">=" ),
          MetricRollupRule("r1", "server1", "disk", "mean", 90, ">=" )
        ), 5).show(truncate = false)
      }
    }
    stopMetricSend()
    spark.streams.active.foreach(_.stop())
  }
}

package com.skt.tcore

import java.util.concurrent.{Executors, TimeUnit}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

import scala.async.Async.async
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object AlarmMonitoring {
  var spark: SparkSession = _
  var instance: AlarmMonitoring = _

  def setSparkSession(spark: SparkSession): Unit = {
    this.spark = spark
    instance = new AlarmMonitoring()(spark)
  }

  def apply(): AlarmMonitoring = {
    if (instance == null) throw new RuntimeException("SparkSession not init..")
    instance
  }
}

class AlarmMonitoring(implicit spark: SparkSession) {

  def startConsoleView(): Unit = {
    val executor = Executors.newScheduledThreadPool(2)

    executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        System.out.println("###################  metric_state_view  ##################################")
        spark.sql(s"SELECT nodegroup, resource, timestamp, metric, value FROM metric_state_view ORDER BY nodegroup, resource, metric").show(truncate = false)
      }
    }, 0, 20, TimeUnit.SECONDS)

    executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        System.out.println("###################  Metric(metricRollup)  ##################################")
        spark.sql(s"SELECT * FROM metric_rollup_view").printSchema()

        val metricRollup = spark
          .sql("SELECT window, resource, metric, cnt, mean, min, max, stddev FROM metric_rollup_view")
          .orderBy("window", "resource", "metric")

        println("######## metricRollup #############")
        metricRollup.printSchema()
        metricRollup.show(truncate = false)
      }
    }, 3, 20, TimeUnit.SECONDS)

    //    executor.scheduleAtFixedRate(new Runnable {
    //      override def run(): Unit = {
    //        getMetricSummary("server1", "cpu", 5)
    //          .foreach(println)
    //      }
    //    }, 5, 20, TimeUnit.SECONDS)
  }


  def getMetricSummary(resource: String, metric: String, window: Int)(implicit spark: SparkSession): Future[Double] = {
    import spark.implicits._

    async {
      println("######## getMetricSummary #############")
      val summaryDF = spark
        .sql("SELECT window, resource, key, cnt, mean, min, max, stddev FROM metric_rollup_view")
        .withColumn("timediff", unix_timestamp(current_timestamp()) - unix_timestamp($"window.start"))
        //.filter((current_timestamp() - unix_timestamp($"window.start")) <= window * 1000)
        .where(s"resource = '${resource}' AND key = '${metric}' AND timediff <= ${window * 60}")
        .groupBy("resource", "key")
        .agg(
          sum($"cnt").as("total_cnt"),
          AlramUdf.metricMean(collect_list(struct("mean", "cnt"))).as("mean"),
          min($"timediff"), max($"timediff")
        )
      //.agg(collect_list(struct("stddev", "cnt"))).as("stddev")
      //.select("resource", "key", "cnt")
      summaryDF.printSchema()
      summaryDF.show()

      val result = summaryDF.head() match {
        case Row(resource: String, key: String, total_cnt: Long, mean: Double, diff_min: Long, diff_max: Long) => mean
        case _ => 0
      }
      result
    }
  }


}

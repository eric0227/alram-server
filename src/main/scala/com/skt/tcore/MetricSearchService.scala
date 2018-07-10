package com.skt.tcore

import java.sql.Timestamp

import com.skt.tcore.AlarmServer.log
import com.skt.tcore.model.{Metric, MetricRule, MetricRuleResult}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.async
import scala.concurrent.Future

object MetricSearchService extends Logging {

  def getCurrentMetricValue(m: Metric)(implicit spark: SparkSession): Future[Option[Double]] = {
    import spark.implicits._
    async {
      spark
        .sql("SELECT resource, metric, value, timestamp FROM metric_state_view")
        .where(s"resource = '${m.resource}' AND metric = '${m.metric}'")
        .map(_.getAs[Double]("value")).take(1).headOption
    }
  }
  def getCurrentMetricValue(query: String)(implicit spark: SparkSession): Future[Option[Double]] = {
    import spark.implicits._
    async {
      spark.sql(query).map(_.getAs[Double]("value")).take(1).headOption
    }
  }

  def selectCurrentMetric(query: String)(implicit spark: SparkSession): DataFrame = {
    spark.sql(query)
  }
  def selectCurrentMetric(metricList: List[Metric])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val metricDf = metricList.toDF()
    val keyFilter = metricList.foldLeft("")((result, r) => result + " OR " + r.condition).substring(3)
    log.info(s"keyFilter::$keyFilter")

    spark
      .sql("SELECT metric.resource, metric.metric, metric.value, metric.timestamp FROM metric_state_view as metric")
      .where(keyFilter)
      .join(metricDf.as("rule"), expr("metric.resource = rule.resource AND metric.metric = rule.metric"))
  }

  def selectCurrentMetricRule(ruleList: List[MetricRule])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val ruleDf = ruleList.toDF()
    val keyFilter = ruleList.foldLeft("")((result, r) => result + " OR " + r.metricCondition).substring(3)
    val valueFilter = ruleList.foldLeft("")((result, r) => result + " OR " + r.condition).substring(3)

    log.info(s"keyFilter::$keyFilter")
    log.info(s"valueFilter::$valueFilter")

    spark
      .sql("SELECT metric.resource, metric.metric, metric.value, metric.timestamp FROM metric_state_view as metric")
      .withColumn("detect", expr(s"CASE WHEN ${valueFilter} THEN true ELSE false END"))
      .where(keyFilter)
      .join(ruleDf.as("rule"), expr("metric.resource = rule.resource AND metric.metric = rule.metric"))
  }


  def getMetricWindow(resource: String, metric: String, window: Int)(implicit spark: SparkSession): Future[Double] = {
    import spark.implicits._

    async {
      println("*** getMetricSummary")
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

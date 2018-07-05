package com.skt.tcore

import java.util.concurrent.{Executors, TimeUnit}

import com.skt.tcore.AlarmMonitoring.spark
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.async
import scala.concurrent.Future

object AlarmMonitoring {
  var spark: SparkSession = _
  var instance: AlarmMonitoring  = _

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
  import spark.implicits._

  def startConsoleView(): Unit = {
    val executor = Executors.newScheduledThreadPool(2)

    executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        System.out.println("###################  metricLatest  ##################################")
        spark.sql(s"SELECT nodegroup, resource, timestamp, metric FROM metricLatest ORDER BY nodegroup, resource").show(truncate = false)
      }
    }, 0, 20, TimeUnit.SECONDS)

    executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        System.out.println("###################  metricState  ##################################")
        spark.sql(s"SELECT * FROM metricState").printSchema()

        val metricStateSummay = spark
          .sql("SELECT window, resource, key, cnt, mean, min, max, stddev FROM metricState")
          .withColumn("summary", struct($"cnt", $"mean", $"min", $"max", $"stddev"))
          .groupBy($"window", $"resource")
          .agg(collect_list(map($"key", $"summary")).as("metric"))
          .select($"window", $"resource", $"metric")
          .orderBy($"window", $"resource")

        println("######## metricStateSummay #############")
        metricStateSummay.printSchema()
        metricStateSummay.show(truncate = false)
      }
    }, 3, 20, TimeUnit.SECONDS)

    executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        getMetricSummary("server1", "cpu", 5)
          .foreach(println)
      }
    }, 5, 20, TimeUnit.SECONDS)
  }


  def getMetricSummary(resource: String, metric: String, window: Int)(implicit spark: SparkSession): Future[Double] = {
    import spark.implicits._

    async {
      println("######## getMetricSummary #############")
      val summaryDF = spark
        .sql("SELECT window, resource, key, cnt, mean, min, max, stddev FROM metricState")
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
        case Row(resource: String, key: String, total_cnt: Long, mean: Double, diff_min:Long, diff_max:Long) => mean
        case _ => 0
      }
      result
    }
  }


}

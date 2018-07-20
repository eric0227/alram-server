package test

import java.io.File

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.skt.tcore.{AlarmServer, common}
import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import com.skt.tcore.common.Common._

class DynamicJoinTest extends FunSuite with SparkSessionTestWrapper with DatasetComparer {

  case class MetricRule(resource: String, metric: String, value: Double, op: String = ">")

  test("DynamicFilterTest") {
    import spark.implicits._

    val eventStreamDF = AlarmServer.readKafkaDF(kafkaServers, eventTopic)

    eventStreamDF.printSchema()
    val streamDf = AlarmServer.selectMetricEventDF(eventStreamDF)
    streamDf.printSchema()

    new Thread( new Runnable {
      override def run(): Unit = {

        var df: DataFrame = null

        (1 to 100) foreach { i =>
          val r = scala.util.Random
          val ruleList = Seq(
             MetricRule("server1", "cpu", r.nextInt(100), ">")
            ,MetricRule("server1", "mem", r.nextInt(100), ">")
          )
          if(df != null) df.unpersist()
          df = spark.sqlContext.createDataFrame(ruleList)//.cache()
          df.createOrReplaceTempView("metric_rule")
          spark.sql("select * from metric_rule").show(truncate = false)
          Thread.sleep(5000)
        }
      }
    }).start()

    spark.sql(
      """
        | select m.*,
        |        case when r.op = '=' and m.value = r.value then 1
        |             when r.op = '>' and m.value > r.value then 1
        |             when r.op = '<' and m.value < r.value then 1
        |        else 0 end chk
        | from metric m, metric_rule r
        | where m.resource = r.resource and m.metric = r.metric
        |
      """.stripMargin)
      .writeStream
      .format("console")
      .option("header", "true").option("truncate", false)
      .start()

    startMetricSend(10, 10)
    Thread.sleep(30 * 1000)
    stopMetricSend()
  }
}

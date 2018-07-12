package test

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.skt.tcore.AlarmServer
import com.skt.tcore.common.Common
import org.apache.spark.sql.functions.window
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import scala.concurrent.duration._

class WatermarkTest extends FunSuite with SparkSessionTestWrapper with DatasetComparer {

  val eventStreamDF = AlarmServer.readKafkaDF(bootstrap, eventTopic)
  eventStreamDF.printSchema()

  test("watermark") {
    import spark.implicits._

    val metricDF = AlarmServer.selectMetricEventDF(eventStreamDF)
    metricDF.printSchema()
//    Common.printConsole(metricDF)


    metricDF
      .withWatermark("timestamp", "20 seconds")
      .groupBy(window($"timestamp", "10 seconds"), $"nodegroup", $"resource", $"metric")
      .agg(last("value").as("value"))
      .writeStream
      .format("console").option("header", "true").option("truncate", false)
      .trigger(Trigger.ProcessingTime(20.seconds))
      .outputMode(OutputMode.Append())
      .start()

    startMetricSend()
    Thread.sleep(1000 * 120)
    stopMetricSend()
  }
}

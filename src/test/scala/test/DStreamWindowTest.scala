package test

import java.util.concurrent.{Executors, TimeUnit}

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructType, TimestampType}
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object DStreamWindowTest {

  val bootstrap = "192.168.203.105:9092"
  val eventTopic = "event"

  val metricSchema = new StructType()
    .add("nodegroup", StringType, true)
    .add("resource", StringType, true)
    .add("metric", StringType, true)
    .add("value", DoubleType, true)
    .add("timestamp", TimestampType, true)

  def createRandomMetrics(resourceCnt: Int = 10): Seq[String] = {
    val r = scala.util.Random
    val resource = "server" + (1 + r.nextInt(resourceCnt))
    val metric = List("cpu", "mem", "disk", "net", "gpu", "power")
    metric.map { m =>
      val value = r.nextInt(100)
      s"""{"nodegroup":"g1", "resource":"$resource", "metric":"$m", "value" : $value}"""
    }
  }

  val executor = Executors.newScheduledThreadPool(2)
  def startMetricSend(serverCnt: Int=10, sleep: Long=1) {
    println("send kafka ..")
    executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        createRandomMetrics(serverCnt).foreach(m => MetricKafkaProducer.send(eventTopic, "k1", m))
      }
    }, 0, sleep, TimeUnit.MILLISECONDS)
  }

  def stopMetricSend(): Unit = {
    println("kafka end")
    executor.shutdown()
  }


  case class Event(nodegroup: String,resource: String,metric: String,value: Double, timestamp: Long)
  case class AlarmRule(id: String, event: String, metric: Double, op: String)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("DStreamWindowTest")
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")

    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("_checkpoint/DStreamWindowTest")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bootstrap,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(eventTopic)

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val event = stream.map(record => (record.timestamp, record.key, record.value)).map { d =>
      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      val event = mapper.readValue[Event](d._3, classOf[Event])
      event.copy(timestamp = d._1)
    }.cache()
    event.count().print()

    (5 to 10) foreach { window =>
      event
        .map(e => (e.metric, e))
        .groupByKeyAndWindow(Seconds(window), Seconds(5))
        .map { case (key, iter) =>
          (key, iter.size, iter.map(_.value).sum)
        }.print()
    }

//    event
//      .window(Seconds(5))
//      .print(num = 1000)

//
//    event
//      .map(e => (e.resource + ":" + e.metric, e))
//      .groupByKeyAndWindow(Duration(3 * 1000))
//      .map { case (key, iter) =>
//        (key, iter.size)
//      }.print(num = 1000)

    startMetricSend(1, 1)

    ssc.start()
    ssc.awaitTermination()
  }
}

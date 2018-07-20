package test

import java.io.File
import java.util.concurrent.{Executors, TimeUnit}

import com.skt.tcore.AlarmServer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructType, TimestampType}
import com.skt.tcore.common.Common._

trait SparkSessionTestWrapper {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("spark test")
      .config("spark.sql.streaming.checkpointLocation",  checkpointPath+"/SparkSessionTest")
      .getOrCreate()
  }

  implicit val sc = spark.sqlContext
  implicit val sp = spark

  val metricSchema = new StructType()
    .add("nodegroup", StringType, true)
    .add("resource", StringType, true)
    .add("metric", StringType, true)
    .add("value", DoubleType, true)
    .add("timestamp", TimestampType, true)

  def createRandomMetrics(resourceCnt: Int = 10): Seq[(String, String)] = {
    val r = scala.util.Random
    val id = if(resourceCnt > 1) 1 + r.nextInt(resourceCnt) else 1
    val resource = "server" + id
    val metric = List("cpu")//, "mem", "disk", "net", "gpu", "power")
    metric.map { m =>
      val value = r.nextInt(100)
      (resource, s"{'nodegroup':'g1', 'resource':'$resource', 'metric':'$m', 'value' : $value}")
    }
  }

  val executor = Executors.newScheduledThreadPool(1)
  def startMetricSend(serverCnt: Int=10, sleep: Long=1) {
    println("send kafka ..")
    var count = 0
    executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        createRandomMetrics(serverCnt).foreach { m =>
          MetricKafkaProducer.send(eventTopic, m._1, m._2)
          count = count + 1
          if(count % 1000 == 0) println(count)
        }
      }
    }, 0, sleep, TimeUnit.MILLISECONDS)
  }

  def stopMetricSend(): Unit = {
    println("kafka end")
    executor.shutdown()
  }
}

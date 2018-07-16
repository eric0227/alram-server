package test

import java.io.File
import java.util.concurrent.{Executors, TimeUnit}

import com.skt.tcore.AlarmServer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructType, TimestampType}

trait SparkSessionTestWrapper {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("spark test")
      .config("spark.sql.streaming.checkpointLocation",  "_checkpoint/SparkSessionTest")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
  }

  implicit val sc = spark.sqlContext
  implicit val sp = spark

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
      s"{'nodegroup':'g1', 'resource':'$resource', 'metric':'$m', 'value' : $value}"
    }
  }

  val executor = Executors.newScheduledThreadPool(2)
  def startMetricSend(serverCnt: Int=10, sleep: Long=1) {
    println("send kafka ..")
    executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        createRandomMetrics(serverCnt).foreach(m => MetricKafkaProducer.sendKafka(eventTopic, "k1", m))
      }
    }, 0, sleep, TimeUnit.MICROSECONDS)
  }

  def stopMetricSend(): Unit = {
    println("kafka end")
    executor.shutdown()
  }

}

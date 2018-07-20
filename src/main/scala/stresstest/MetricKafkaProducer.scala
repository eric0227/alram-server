package stresstest

import java.util.concurrent.Future
import java.util.{Date, Properties}

import com.skt.tcore.common.Common._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

object MetricKafkaProducer  {

  val BOOTSTRAP_SERVERS = kafkaServers
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)


  def send(topic: String, key: String="", data: String) = {
    val record = new ProducerRecord[String, String](topic, key, data)
    val future = producer.send(record)
    future
    //val metadata = future.get()
    //printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) \n", record.key, record.value, metadata.partition, metadata.offset)
  }

  def createRandomMetrics(resourceCnt: Int, metricList: List[String]): Seq[(String, String)] = {
    val r = scala.util.Random
    val id = if(resourceCnt > 1) 1 + r.nextInt(resourceCnt) else 1
    val resource = "server" + id

    metricList.map { m =>
      val value = r.nextInt(100)
      (resource, s"{'nodegroup':'g1', 'resource':'$resource', 'metric':'$m', 'value' : $value}")
    }
  }

  def sendRandomMetric(sendCount: Int, serverCount: Int = 10, metricList: List[String] = List("cpu", "mem", "disk", "net", "gpu", "power")): Unit = {
    var count = 0
    var future: Future[RecordMetadata] = null
    (1 to sendCount) foreach { i =>
      createRandomMetrics(serverCount, metricList).foreach { m =>
        future = MetricKafkaProducer.send(metricTopic, m._1, m._2)
        count = count + 1
        if (count % 10000 == 0) {
          val metadata = future.get()
          println(new Date().toString, s"count: $count, meta(partition=${metadata.partition}, offset=${metadata.offset})")
        }
      }
    }

    if(future != null) {
      val metadata = future.get()
      println(new Date().toString, s"count: $count, meta(partition=${metadata.partition}, offset=${metadata.offset})")
    }
  }

  def main(args: Array[String]): Unit = {
    val count = if(args.length == 1) args(0).toInt else 1000
    println("send count :: " + count)

    val start = new Date()
    println("start",start.toString, start.getTime)
    watchTime("sendRandomMetric") {
      sendRandomMetric(sendCount = count, serverCount = 100, metricList = List("cpu"))
    }
    val end = new Date()
    println("start", end.toString, end.getTime)
    println(end.getTime - start.getTime)
  }
}

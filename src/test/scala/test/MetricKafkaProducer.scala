package test

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.scalatest.FunSuite

object MetricKafkaProducer  {

  val BOOTSTRAP_SERVERS = "192.168.203.105:9092"
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)


  def sendKafka(topic: String, key: String="", data: String): Unit = {
    val record = new ProducerRecord[String, String](topic, key, data)
    val future = producer.send(record)
    val metadata = future.get()
    //printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) \n", record.key, record.value, metadata.partition, metadata.offset)
  }
}

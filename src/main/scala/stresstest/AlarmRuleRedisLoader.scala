package stresstest

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.skt.tcore.common.{Common, RedisClient}
import io.lettuce.core.pubsub.RedisPubSubAdapter
import stresstest.AlarmDetectionStressRddJoinTest.createRuleDF

import scala.collection.JavaConverters._

object AlarmRuleRedisLoader {
  def apply[T](f:(Seq[MetricRule]) => T) = new AlarmRuleRedisLoader[T](f)

  def main(args: Array[String]): Unit = {
    AlarmRuleRedisLoader { list =>
      println("load : "+ list.size)
    }

    Thread.sleep(1000 * 1000)
  }
}

class AlarmRuleRedisLoader[T](f:(Seq[MetricRule]) => T) {

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  val redisConn = RedisClient.getInstance().client.connectPubSub()
  redisConn.addListener(new RedisPubSubAdapter[String, String] {
    override def message(channel: String, message: String): Unit = {
      super.message(channel, message)
      println("message :: " + message)
      val start = System.currentTimeMillis()
      loadRedisRule()
      val end = System.currentTimeMillis()
      println("loadRedisRule => " + (end - start) + "ms")
    }
  })
  val redisCmd = redisConn.sync()
  redisCmd.subscribe(Common.metricRuleSyncChannel)

  val redis = RedisClient.getInstance().client.connect().async()
  def loadRedisRule(): Unit = {
    val ruleList = Common.watchTime("redis rule sync") {
      val list = redis.keys(Common.metricRuleKey+":*").get.asScala.toList.distinct.map { k =>
        redis.hgetall(k)
      }.flatMap { f =>
        f.get().values().asScala
      }
      println("load rule :: " + list.size)

      val ruleList = list.map { json =>
        mapper.readValue[MetricRule](json, classOf[MetricRule])
      }
      ruleList
    }
    f(ruleList)
  }
  //loadRedisRule()
}

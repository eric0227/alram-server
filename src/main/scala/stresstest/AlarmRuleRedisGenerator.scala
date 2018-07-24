package stresstest

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.skt.tcore.common.{Common, RedisClient}
import scala.collection.JavaConverters._

object AlarmRuleRedisGenerator extends App {
  val count = if(args.length == 1) args(0).toInt else 1000
  println("count :: " + count)

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  val redis = RedisClient.getInstance().client.connect().async()
  val start = System.currentTimeMillis()

  Common.watchTime("delete") {
    val keys = redis.keys(Common.metricRuleKey + ":*").get.asScala.distinct
    keys.foreach { k =>
      redis.del(k).get()
    }
  }
  //redis.del(Common.metricRuleKey + "*").get()

  val r = scala.util.Random
  Common.watchTime("hset") {
    (1 to count).map { i =>
      val rule = MetricRule("tcore-oi-data-2-" + i, "mem_used_percent", r.nextInt(100), ">")
      redis.hset(rule.rkey(Common.metricRuleKey), rule.rfield, mapper.writeValueAsString(rule))
    }.foreach(f => f.get())
  }

  Common.watchTime("publish") {
    val pub = RedisClient.getInstance().client.connectPubSub().sync()
    pub.publish(Common.metricRuleSyncChannel, "sync")
  }

  val end = System.currentTimeMillis()
  println(end - start + "ms")
}

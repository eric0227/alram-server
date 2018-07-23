package stresstest

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.skt.tcore.common.{Common, RedisClient}

object AlarmRuleRedisGenerator extends App {
  val count = if(args.length == 1) args(0).toInt else 1000
  println("count :: " + count)

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  val redis = RedisClient.getInstance().redis
  redis.del(Common.metricRuleKey)

  val r = scala.util.Random
  val start = System.currentTimeMillis()
  (1 to count).foreach { i =>
    val rule = MetricRule("server" + i, "cpu", r.nextInt(100), ">")
    val key = rule.resource+":"+rule.metric
    redis.hset(Common.metricRuleKey, key, mapper.writeValueAsString(rule))
  }
  val pub = RedisClient.getInstance().client.connectPubSub().sync()
  pub.publish(Common.metricRuleSyncChannel, "sync")
  val end = System.currentTimeMillis()
  println(end - start + "ms")
}

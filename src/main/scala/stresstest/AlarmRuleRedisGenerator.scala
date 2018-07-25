package stresstest

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.skt.tcore.common.{Common, RedisClient}
import scala.collection.JavaConverters._

object AlarmRuleRedisGenerator extends App {
  val serverCount = if(args.length > 0) args(0).toInt else 1000
  val loopCount = if(args.length > 1) args(1).toInt else 10
  val metric = if(args.length > 2) args(2) else "mem.used_percent"
  println("server count :: " + serverCount)
  println("loop count :: " + loopCount)

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  val redis = RedisClient.getInstance().client.connect().async()
  val pub = RedisClient.getInstance().client.connectPubSub().sync()
  val r = scala.util.Random

  val start = System.currentTimeMillis()
  (1 to loopCount) foreach { loop =>
    val start = System.currentTimeMillis()
    val keys = redis.keys(Common.metricRuleKey + ":*").get.asScala.distinct
    keys.map(k => redis.del(k)).foreach(f => f.get())

    (1 to serverCount).map { i =>
      val rule = MetricRule("tcore-oi-data-2-" + i, metric, r.nextInt(100), ">")
      redis.hset(rule.rkey(Common.metricRuleKey), rule.rfield, mapper.writeValueAsString(rule))
    }.foreach(f => f.get())

    pub.publish(Common.metricRuleSyncChannel, "sync")

    val end = System.currentTimeMillis()
    println(s"loop : $loop, size : $serverCount,  ${end - start}ms")
  }

  val end = System.currentTimeMillis()
  println(s"\ntotal : ${end - start}ms")

}

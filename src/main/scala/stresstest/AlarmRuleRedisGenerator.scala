package stresstest

import java.util.UUID

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.skt.tcore.common.{Common, RedisClient}

import scala.collection.JavaConverters._

object AlarmRuleRedisGenerator extends App {
  val serverCount = if(args.length > 0) args(0).toInt else 100
  val loopCount = if(args.length > 1) args(1).toInt else 1
  val metrics = if(args.length > 2) args(2) else "mem.used_percent"
  println("server count :: " + serverCount)
  //println("loop count :: " + loopCount)

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  val redis = RedisClient.getInstance().client.connect().async()
  val pub = RedisClient.getInstance().client.connectPubSub().sync()
  val r = scala.util.Random

  var count = 0
  (1 to loopCount) foreach { loop =>
    val metricList = metrics.split(",")
    metricList.foreach { metric =>
      count = count + 1
      val start = System.currentTimeMillis()
      System.err.println(s"#start => server count : $serverCount, metric : ${metric}, timestamp: ${start}")

      val keys = redis.keys(Common.metricRuleKey + ":*").get.asScala.distinct
      keys.map(k => redis.del(k)).foreach(f => f.get())

      (1 to serverCount).map { i =>
        val rule = MetricRule("tcore-oi-data-2-" + i, metric, r.nextInt(100), ">")
        redis.hset(rule.rkey(Common.metricRuleKey), rule.rfield, mapper.writeValueAsString(rule))
      }.foreach(f => f.get())

      val uuid = UUID.randomUUID().toString()
      pub.publish(Common.metricRuleSyncChannel, "sync:"+uuid)
      println(s"pub.sync metric:$metric")

      val end = System.currentTimeMillis()
      println(s"server count : $serverCount, metric : ${metric}, time: ${end - start}ms, timestamp: ${end}")
      System.err.println(s"#end => server count : $serverCount, metric : ${metric}, time: ${end - start}ms, timestamp: ${end}")
      System.err.println()

      if(loopCount > 1) Thread.sleep(60 * 1000)
    }
  }
}

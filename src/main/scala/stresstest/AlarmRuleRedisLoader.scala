package stresstest

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.lambdaworks.redis.pubsub.RedisPubSubAdapter
import com.skt.tcore.common.{Common, RedisClient}
import org.apache.spark.sql.SparkSession
import stresstest.AlarmRuleRedisGenerator.pub
import stresstest.AlarmRuleRedisLoaderRDD.args

import scala.collection.JavaConverters._

object AlarmRuleRedisLoader {
  def apply[T](f:(Seq[MetricRule]) => T) = new AlarmRuleRedisLoader[T](f)

  def main(args: Array[String]): Unit = {

    val master = if (args.length == 1) Some(args(0)) else None
    val builder = SparkSession.builder().appName("AlarmRuleRedisLoader")
    master.foreach(mst => builder.master(mst))
    implicit val spark = builder.getOrCreate()
    println("start application..")

    import spark.implicits._
    import org.apache.spark.sql._
    AlarmRuleRedisLoader { list =>
    }
    Thread.sleep(100000 * 1000)
  }
}

class AlarmRuleRedisLoader[T](f:(Seq[MetricRule]) => T) {
//  val mapper = new ObjectMapper() with ScalaObjectMapper
//  mapper.registerModule(DefaultScalaModule)
//  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  val redisConn = RedisClient.getInstance().client.connectPubSub()
  val subCmd = redisConn.sync()
  val pubCmd = RedisClient.getInstance().client.connectPubSub().sync()
  redisConn.addListener(new RedisPubSubAdapter[String, String] {
    var count = 0
    var total: Long = 0
    override def message(channel: String, message: String): Unit = synchronized {
      super.message(channel, message)

      val Array(cmd, uuid) = message.split(":")
      if(cmd == "sync") {
        println("redis sub message => " + message)

        val start = System.currentTimeMillis()
        System.err.println(s"#start => sync rule(redis): $count, timestamp: ${start}")

        val list = loadRedisRule()
        val end = System.currentTimeMillis()
        pubCmd.publish(Common.metricRuleSyncChannel, "ack:" + uuid)
        count = count + 1
        total = total + (end - start)
        println(s"sync rule(redis): $count, size: ${list.size}, time: ${end - start}ms, total: ${total}ms, timestamp: ${end}")
        System.err.println(s"#end => sync rule(redis): $count, size: ${list.size}, time: ${end - start}ms, total: ${total}ms, timestamp: ${end}")
        System.err.println()
        println()
      }
    }
  })
  subCmd.subscribe(Common.metricRuleSyncChannel)

  val redis = RedisClient.getInstance().client.connect().async()
  val keys = redis.keys(Common.metricRuleKey+":*").get.asScala.toList.distinct
  println("key size => " + keys.size)
  keys.take(100).map (k => redis.hgetall(k)).flatMap { f => f.get().values().asScala}

  def loadRedisRule(): List[MetricRule] = {
    val start = System.currentTimeMillis()
    val list = redis.keys(Common.metricRuleKey+":*").get.asScala.toList.distinct.map { k =>
      redis.hgetall(k)
    }.flatMap { f =>
      f.get().values().asScala
    }
    val ruleList = list.map { json =>
      //mapper.readValue[MetricRule](json, classOf[MetricRule])
      MetricRule(json)
    }
    val end = System.currentTimeMillis()
    println(s"hget time : ${end - start}ms, count: ${ruleList.size}")
    f(ruleList)
    ruleList
  }
  //loadRedisRule()
}

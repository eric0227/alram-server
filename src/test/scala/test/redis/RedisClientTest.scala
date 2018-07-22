package test.redis


import java.util.concurrent.TimeUnit

import org.scalatest.FunSuite
import redis.clients.jedis.Jedis
import redis.clients.jedis.{HostAndPort, JedisCluster}
import io.lettuce.core
import io.lettuce.core.RedisClient
import io.lettuce.core.pubsub.{RedisPubSubAdapter, RedisPubSubListener}

/**
  * Created by mac on 2018. 7. 22..
  */
class RedisClientTest extends FunSuite {

  test("jedis") {

    val jedis = new Jedis("localhost")
    jedis.set("foo", "bar")
    val value = jedis.get("foo")
    println(value)
  }

  test("rettuce") {
    val client = RedisClient.create("redis://localhost")
    val connection = client.connect()
    val sync = connection.sync()
    val value = sync.get("foo")
    println(value)
  }

  test("sub") {
    val client = RedisClient.create("redis://localhost")
    val conn = client.connectPubSub()

    conn.addListener(new RedisPubSubAdapter[String, String] {
      override def message(channel: String, message: String): Unit = {
        super.message(channel, message)
        println("message :: " + message)
      }
    })
    val sync = conn.async()
    sync.subscribe("pub_test")

    Thread.sleep(1000000)
  }

  test("pub") {
    val client = RedisClient.create("redis://localhost")
    val conn = client.connectPubSub()

    val sync = conn.async()

    Thread.sleep(1000)

    (1 to 10) foreach { i =>
      println(sync.publish("pub_test", i.toString).get())
      Thread.sleep(10000)
    }
  }

}

package test.redis


import java.util.concurrent.TimeUnit

import com.adendamedia.salad.SaladAPI
import com.adendamedia.salad.dressing.SaladStringKeyAPI
import com.adendamedia.salad.serde.SnappySerdes
import com.lambdaworks.redis.cluster.RedisClusterClient
import com.lambdaworks.redis.codec.ByteArrayCodec
import com.lambdaworks.redis.{RedisClient, RedisURI}
import com.lambdaworks.redis.pubsub.RedisPubSubAdapter
import org.scalatest.FunSuite
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by mac on 2018. 7. 22..
  */
class RedisClientTest extends FunSuite {

  test("lettuce") {
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
    val sync = conn.sync()
    sync.subscribe("pub_test")

    Thread.sleep(100000)

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


  test("lettuce cluster") {
    val node1 = RedisURI.create("192.168.203.101", 7001)
    val node2 = RedisURI.create("192.168.203.101", 7002)
    val node3 = RedisURI.create("192.168.203.101", 7003)

    import java.util
    val client = RedisClusterClient.create(util.Arrays.asList(node1, node2, node3))

    val connection = client.connect()
    val sync = connection.sync()
    sync.set("foo", "test")
    val value = sync.get("foo")
    println(value)

    sync.hset("hset", "aa", "aa11")
    sync.hset("hset", "bb", "bb11")
    println(sync.hgetall("hset"))
  }
}

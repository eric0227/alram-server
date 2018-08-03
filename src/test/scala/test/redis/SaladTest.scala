package test.redis

import com.adendamedia.salad.SaladAPI
import com.adendamedia.salad.dressing.SaladStringKeyAPI
import com.adendamedia.salad.serde.SnappySerdes
import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.cluster.RedisClusterClient
import com.lambdaworks.redis.codec.ByteArrayCodec
import com.skt.tcore.common.Common._
import org.scalatest.FunSuite

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

class SaladTest extends FunSuite {

  def getRedisClusterClient() = {
    val node1 = RedisURI.create("192.168.203.101", 7001)
    val node2 = RedisURI.create("192.168.203.101", 7002)
    val node3 = RedisURI.create("192.168.203.101", 7003)

    import java.util
    val client = RedisClusterClient.create(util.Arrays.asList(node1, node2, node3))
    client
  }

  test("lettuce sync") {
    val client = getRedisClusterClient()
    //val lettuce = client.connect().async()

    val lettuceAPI = client.connect().sync()

    val count = 10000
    watchTime("del") {
      (1 to count).map { i => lettuceAPI.del("async_key_" + i) }
    }

    watchTime("set") {
      (1 to count).map { i => lettuceAPI.set("async_key_" + i, "1") }
    }

    watchTime("get") {
      println(
        (1 to count).map { i => lettuceAPI.get("async_key_" + i) }.map { _.toInt}.sum
      )
    }
  }


  test("lettuce async") {
    val client = getRedisClusterClient()
    //val lettuce = client.connect().async()

    val lettuceAPI = client.connect().async()

    val count = 10000
    watchTime("del") {
      (1 to count).map { i => lettuceAPI.del("async_key_" + i) }.foreach(_.get)
    }

    watchTime("set") {
      (1 to count).map { i => lettuceAPI.set("async_key_" + i, "1") }.foreach(_.get)
    }

    watchTime("get") {
      println(
        (1 to count).map { i => lettuceAPI.get("async_key_" + i) }.map { _.get().toInt}.sum
      )
    }
  }

  test("SaladAPI sync") {
    import SnappySerdes._
    val client = getRedisClusterClient()
    //val lettuce = client.connect().async()

    val lettuceAPI = client.connect(ByteArrayCodec.INSTANCE).async()
    val saladAPI = new SaladStringKeyAPI(new SaladAPI(lettuceAPI))

    val count = 10000
    watchTime("del") {
      (1 to count).map { i => Await.result(saladAPI.del("async_key_" + i), Duration.Inf) }
    }

    watchTime("set") {
      (1 to count).map { i => Await.result(saladAPI.set("async_key_" + i, "1"), Duration.Inf) }
    }

    watchTime("get") {
      println(
        (1 to count).map { i => saladAPI.get[String]("async_key_" + i) }.map { f =>
          Await.result(f.map(_.getOrElse("0").toInt), Duration.Inf)
        }.sum
      )
    }
  }

  test("SaladAPI async") {
    import SnappySerdes._
    val client = getRedisClusterClient()
    //val lettuce = client.connect().async()

    val lettuceAPI = client.connect(ByteArrayCodec.INSTANCE).async()
    val saladAPI = new SaladStringKeyAPI(new SaladAPI(lettuceAPI))

    val count = 10000
    watchTime("del") {
      Await.ready(
        Future.sequence(
          (1 to count).map { i => saladAPI.del("async_key_" + i) }
        ), Duration.Inf)
    }

    watchTime("set") {
      Await.ready(
        Future.sequence(
          (1 to count).map { i => saladAPI.set("async_key_" + i, "1") }
        ), Duration.Inf)
    }

    watchTime("get") {
      println(
        Await.result(
          Future.sequence(
            (1 to count).map { i => saladAPI.get[String]("async_key_" + i) }.map { f =>
              f.map(_.getOrElse("0").toInt)
            }), Duration.Inf
        ).sum
      )
    }
  }
}


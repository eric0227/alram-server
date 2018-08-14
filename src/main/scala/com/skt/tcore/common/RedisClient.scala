package com.skt.tcore.common

import com.adendamedia.salad.SaladAPI
import com.adendamedia.salad.dressing.{SaladStringKeyAPI, SaladUIIDKeyAPI}
import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.cluster.RedisClusterClient
import com.lambdaworks.redis.codec.ByteArrayCodec
import com.skt.tcore.common.Common.redisServers

//object RedisClient {
//  @volatile lazy val instance: RedisClient = new RedisClient()
//}

object RedisClient {
  import scala.collection.JavaConverters._

  val redisServerList = redisServers.split(",").map { token =>
    val Array(ip, port) = token.split(":")
    RedisURI.create(ip, port.toInt)
  }.toSet.asJava

  val client = RedisClusterClient.create(redisServerList)
  val syncApi = RedisClusterClient.create(redisServerList).connect().sync()
  val asyncApi = RedisClusterClient.create(redisServerList).connect().async()

  val lettuceAPI = client.connect(ByteArrayCodec.INSTANCE).async()
  val scalaApi = new SaladStringKeyAPI(new SaladAPI(lettuceAPI))

}

package com.skt.tcore.common

import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.cluster.RedisClusterClient
import com.skt.tcore.common.Common.redisServers

object RedisClient {

  @volatile var instance: RedisClient = _

  def getInstance(): RedisClient = synchronized {
    if (instance == null) {
      instance = new RedisClient()
    }
    instance
  }
}

class RedisClient {

  import scala.collection.JavaConverters._

  val redisServerList = redisServers.split(",").map { token =>
    val Array(ip, port) = token.split(":")
    RedisURI.create(ip, port.toInt)
  }.toSet.asJava

  val client = RedisClusterClient.create(redisServerList)
  val redis = RedisClusterClient.create(redisServerList).connect().sync()

}

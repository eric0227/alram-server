package com.skt.tcore.common

import com.skt.tcore.common.Common.redisServers
import redis.clients.jedis.{HostAndPort, JedisCluster}

object RedisClient {

  @volatile var instance: RedisClient = _
  def getInstance(): RedisClient = synchronized {
    if(instance == null) {
      instance = new RedisClient()
    }
    instance
  }
}

class RedisClient {
  import scala.collection.JavaConverters._

  val redisServerList = redisServers.split(",").map { token =>
    val Array(ip, port) = token.split(":")
    new HostAndPort(ip, port.toInt)
  }.toSet.asJava
  println(redisServerList)

  val redis = new JedisCluster(redisServerList)
}

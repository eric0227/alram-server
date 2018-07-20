package test


import org.scalatest.FunSuite
import redis.clients.jedis.{HostAndPort, JedisCluster}

class RedisTest extends FunSuite {

  val redisServers = Seq(("server01", 7001),("server01", 7002),("server01", 7003))
/*
  test("ping pong") {
    implicit val akkaSystem = akka.actor.ActorSystem()
    val redis = RedisCluster(redisServers.map(p=>RedisServer(p._1,p._2)))
    val futurePong = redis.ping()
    println(Await.result(futurePong, 5 seconds))

    val echo = redis.echo("test")
    println(Await.result(echo, 5 seconds))
  }

  test("RedisComputeSlot") {
    assert(RedisComputeSlot.hashSlot("foo") == 12182)
    assert(RedisComputeSlot.hashSlot("somekey") == 11058)
    assert(RedisComputeSlot.hashSlot("somekey3452345325453532452345") == 15278)
    assert(RedisComputeSlot.hashSlot("rzarzaZERAZERfqsfsdQSFD") == 14258)
    assert(RedisComputeSlot.hashSlot("{foo}46546546546") == 12182)
    assert(RedisComputeSlot.hashSlot("foo_312312") == 5839)
    assert(RedisComputeSlot.hashSlot("aazaza{aa") == 11473)
  }

  test("encoding") {
    val clusterSlotsAsByteString = ByteString(new sun.misc.BASE64Decoder().decodeBuffer("KjMNCio0DQo6MA0KOjU0NjANCiozDQokOQ0KMTI3LjAuMC4xDQo6NzAwMA0KJDQwDQplNDM1OTlkZmY2ZTNhN2I5ZWQ1M2IxY2EwZGI0YmQwMDlhODUwYmE1DQoqMw0KJDkNCjEyNy4wLjAuMQ0KOjcwMDMNCiQ0MA0KYzBmNmYzOWI2NDg4MTVhMTllNDlkYzQ1MzZkMmExM2IxNDdhOWY1MA0KKjQNCjoxMDkyMw0KOjE2MzgzDQoqMw0KJDkNCjEyNy4wLjAuMQ0KOjcwMDINCiQ0MA0KNDhkMzcxMjBmMjEzNTc4Y2IxZWFjMzhlNWYyYmY1ODlkY2RhNGEwYg0KKjMNCiQ5DQoxMjcuMC4wLjENCjo3MDA1DQokNDANCjE0Zjc2OWVlNmU1YWY2MmZiMTc5NjZlZDRlZWRmMTIxOWNjYjE1OTINCio0DQo6NTQ2MQ0KOjEwOTIyDQoqMw0KJDkNCjEyNy4wLjAuMQ0KOjcwMDENCiQ0MA0KYzhlYzM5MmMyMjY5NGQ1ODlhNjRhMjA5OTliNGRkNWNiNDBlNDIwMQ0KKjMNCiQ5DQoxMjcuMC4wLjENCjo3MDA0DQokNDANCmVmYThmZDc0MDQxYTNhOGQ3YWYyNWY3MDkwM2I5ZTFmNGMwNjRhMjENCg=="))
    val clusterSlotsAsBulk: DecodeResult[RedisReply] = RedisProtocolReply.decodeReply(clusterSlotsAsByteString)
    var decodeValue = ""
    val dr: DecodeResult[String] = clusterSlotsAsBulk.map({
      case a: MultiBulk =>
        ClusterSlots().decodeReply(a).toString()
      case _ => "fail"
    })

    val r = dr match {
      case FullyDecoded(decodeValue, _) => decodeValue == "Vector(ClusterSlot(0,5460,ClusterNode(127.0.0.1,7000,e43599dff6e3a7b9ed53b1ca0db4bd009a850ba5),Stream(ClusterNode(127.0.0.1,7003,c0f6f39b648815a19e49dc4536d2a13b147a9f50), ?)), " +
        "ClusterSlot(10923,16383,ClusterNode(127.0.0.1,7002,48d37120f213578cb1eac38e5f2bf589dcda4a0b),Stream(ClusterNode(127.0.0.1,7005,14f769ee6e5af62fb17966ed4eedf1219ccb1592), ?)), " +
        "ClusterSlot(5461,10922,ClusterNode(127.0.0.1,7001,c8ec392c22694d589a64a20999b4dd5cb40e4201),Stream(ClusterNode(127.0.0.1,7004,efa8fd74041a3a8d7af25f70903b9e1f4c064a21), ?)))"

      case _ => false
    }
    println(r)
  }

  test("set get") {

    implicit val akkaSystem = akka.actor.ActorSystem()
    val redis = RedisCluster(redisServers.map(p=>RedisServer(p._1,p._2)))
    redis.refreshConnections()
    println(redis.getClusterSlots())
    //println(Await.result(redis.set[String]("foo","FOO"), 5.seconds))
    //assert(Await.result(redis.exists("foo"), 5.seconds))
    //assert(Await.result(redis.get[String]("foo"), 5.seconds).getOrElse("") == "FOO")
  }
*/
  test("jedis test") {
    import scala.collection.JavaConverters._
    import scala.collection.JavaConversions._

    val list = redisServers.map(d => new HostAndPort(d._1, d._2))
    val set: java.util.Set[HostAndPort] =  list.toArray.toSet.asJava
    val c = new JedisCluster(set)
    c.set("foo2", "bar2")
    println(c.get("foo2"))

    c.hset("alarm_rule", "server1:cpu", ">:90")
    println(c.hget("alarm_rule", "server1:cpu"))
  }
}

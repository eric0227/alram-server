package com.skt.tcore.common

import com.typesafe.config._
import org.apache.spark.sql.DataFrame

import scala.concurrent.duration.{Duration, FiniteDuration}

object Common {

  type Getter[T] = (Config, String) => T
  lazy val config: Config = ConfigFactory.load()
  // spark
  lazy val checkpointPath: String = Common.config.getString("spark.checkpointPath")
  // kafka
  lazy val kafkaServers: String = Common.config.getString("kafka.servers")

  implicit val stringGetter: Getter[String] = _ getString _
  implicit val booleanGetter: Getter[Boolean] = _ getBoolean _
  implicit val intGetter: Getter[Int] = _ getInt _
  implicit val doubleGetter: Getter[Double] = _ getDouble _
  implicit val longGetter: Getter[Long] = _ getLong _
  implicit val bytesGetter: Getter[Bytes] = (c, p) => Bytes(c getBytes p)
  implicit val durationGetter: Getter[Duration] = (c, p) => Duration.fromNanos((c getDuration p).toNanos)
  implicit val finiteDurationGetter: Getter[FiniteDuration] = (c, p) => Duration.fromNanos((c getDuration p).toNanos)
  implicit val configListGetter: Getter[ConfigList] = _ getList _
  implicit val configGetter: Getter[Config] = _ getConfig _
  implicit val objectGetter: Getter[ConfigObject] = _ getObject _
  implicit val memorySizeGetter: Getter[ConfigMemorySize] = _ getMemorySize _

  implicit class ConfigOps(val config: Config) extends AnyVal {
    def getOrElse[T: Getter](path: String, defValue: => T): T = opt[T](path) getOrElse defValue

    def opt[T: Getter](path: String): Option[T] = {
      if (config hasPathOrNull path) {
        val getter = implicitly[Getter[T]]
        Some(getter(config, path))
      } else
        None
    }
  }

  // app
  lazy val ruleFilePath: String = Common.config.getString("app.ruleFilePath")
  // kafka
  lazy val metricTopic: String = Common.config.getString("kafka.topics.metric")
  lazy val logTopic: String = Common.config.getString("kafka.topics.log")
  lazy val eventTopic: String = Common.config.getString("kafka.topics.event")
  lazy val alarmTopic: String = Common.config.getString("kafka.topics.alarm")
  lazy val maxOffsetsPerTrigger: Option[Long] = Common.config.opt[Long]("maxOffsetsPerTrigger")
  // redis
  lazy val redisServers: String = Common.config.getString("redis.servers")
  lazy val metricRuleKey: String = Common.config.getString("redis.metricRuleKey")
  lazy val metricRuleSyncChannel: String = Common.config.getString("redis.metricRuleSyncChannel")

  def printConsole(df: DataFrame, numRows: Int = 0): Unit = {
    if (df.isStreaming) {
      val writer = df.writeStream.format("console").option("header", "true").option("truncate", false)
      if (numRows > 0) writer.option("numRows", numRows)
      writer.start()
    } else {
      if (numRows > 0) df.show(numRows, truncate = false) else df.show(truncate = false)
    }
  }

  def watchTime[T](name: String, min: Int = 0)(block: => T): T = {
    val start = System.nanoTime()
    val ret = block
    val end = System.nanoTime()

    import scala.concurrent.duration._
    import scala.language.postfixOps

    val elapsed = (end - start) nanos

    if (elapsed.toMillis > min) {
      println(s"code $name takes ${elapsed.toMillis} millis seconds.")
    }
    ret
  }

  case class Bytes(value: Long)

}

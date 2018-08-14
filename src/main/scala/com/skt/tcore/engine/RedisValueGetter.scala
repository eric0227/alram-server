package com.skt.tcore.engine

import com.adendamedia.salad.serde.SnappySerdes._
import com.skt.tcore.common.Common._
import com.skt.tcore.common.RedisClient._
import com.skt.tcore.engine.CepRule.Metric
import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.collection.JavaConverters._

/**
  * Created by kyu hyoung Lee on 2018-08-14.
  */
object RedisValueGetter {
  object Implicits {
    implicit val metricValueGetter = new RedisMetricValueGetter()
    implicit val alarmValueGetter = new RedisAlarmValueGetter()
  }

  class RedisMetricValueGetter extends MetricValueGetter {
    def range(m: Metric, time: Long): Future[Option[List[Double]]] = {
      val end = System.currentTimeMillis()
      val start = end - time
      async {
        val list = syncApi.zrange(metricValueAggKey+":"+m.resource+":"+m.metric, start, end).asScala.toList
        if (list == null || list.isEmpty) None else Some(list.map(_.toDouble))
      }
    }
    def mean(x: Seq[Double]) = x.sum / x.length
    def stddev(x: Seq[Double]) = math.sqrt((x.map(_ - mean(x)).map(t => t*t).sum) / x.length)

    override def get(m: Metric): Future[Option[Double]] = async {
      val v = asyncApi.hget(metricValueKey+":"+m.resource, m.metric).get()
      if(v == null) None else Some(v.toDouble)
    }
    override def min(m: Metric, time: Long): Future[Option[Double]] = range(m, time).map(_.map(_.min))
    override def max(m: Metric, time: Long): Future[Option[Double]] = range(m, time).map(_.map(_.max))
    override def mean(m: Metric, time: Long): Future[Option[Double]] = range(m, time).map(_.map(mean(_)))
    override def stddev(m: Metric, time: Long): Future[Option[Double]] = range(m, time).map(_.map(stddev(_)))
  }

  class RedisAlarmValueGetter extends AlarmValueGetter {
    override def get(id: String): Future[Option[Double]] = async {
      val v = asyncApi.hget(alarmStateKey+":"+id, "value").get()
      if(v == null) None else Some(v.toDouble)
    }
    override def isAlarm(id: String): Future[Boolean] = async {
      val v = asyncApi.hget(alarmStateKey+":"+id, "alarm").get()
      if(v != null && v == "1") true else false
    }
    override def continuousCount(id: String): Future[Int] =  async {
      val v = asyncApi.hget(alarmStateKey+":"+id, "continuous").get()
      if(v != null && v == "1") v.toInt else 0
    }
  }
}

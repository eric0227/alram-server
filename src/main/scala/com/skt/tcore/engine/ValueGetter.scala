package com.skt.tcore.engine

import com.skt.tcore.engine.CepRule.Metric

import scala.concurrent.Future

/**
  * Created by kyu hyoung Lee on 2018-08-14.
  */
trait MetricValueGetter {
  def get(m: Metric): Future[Option[Double]]
  def min(m: Metric, time: Long): Future[Option[Double]]
  def max(m: Metric, time: Long): Future[Option[Double]]
  def mean(m: Metric, time: Long): Future[Option[Double]]
  def stddev(m: Metric, time: Long): Future[Option[Double]]
}

trait AlarmValueGetter {
  def get(id: String): Future[Option[Double]]
  def isAlarm(id: String): Future[Boolean]
  def continuousCount(id: String): Future[Int]
}


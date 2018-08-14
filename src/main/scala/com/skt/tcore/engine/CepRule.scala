package com.skt.tcore.engine

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

/**
  * Created by kyu hyoung Lee on 2018-08-13.
  */
object CepRule {

  sealed trait Rule {
    def id: String
  }

  sealed trait RuleEvaluation {
    def eval: Future[Option[Boolean]]
  }

  sealed trait RuleExpr extends Rule {
    def and(r: RuleExpr) = And(this, r)
    def &&(r: RuleExpr)  = And(this, r)
    def or(r: RuleExpr)  = Or(this, r)
    def ||(r: RuleExpr)  = Or(this, r)
  }

  final case class And[T <: RuleExpr](l: T, r: T) extends RuleExpr {
    def id: String = l.id+":"+r.id
  }

  final case class Or[T <: RuleExpr](l: T, r: T) extends RuleExpr {
    def id: String = l.id+":"+r.id
  }

  def interpret[T <: RuleExpr](rule: T): Future[Option[Boolean]] = rule match {
    case re: RuleEvaluation => re.eval
    case And(l, r) => for {i <- interpret(l);j <- interpret(r)} yield for {ii <- i; jj <- j} yield ii && jj
    case Or(l, r)  => for {i <- interpret(l);j <- interpret(r)} yield for {ii <- i; jj <- j} yield ii || jj
    case _ => Future(None)
  }

  def serialize[T <: RuleExpr](rule: T): String = ???
  def deserialize[T <: RuleExpr](json: String): T = ???

  import RedisValueGetter.Implicits._
  final case class Metric(resource: String, metric: String, v: Option[Double])(implicit getter: MetricValueGetter) {
    def value: Future[Option[Double]] = v.map(d => Future(Some(d))).getOrElse (getter.get(this))
  }

  final case class MetricRule(id: String, metric: Metric, value: Double, op: String) extends RuleExpr with RuleEvaluation {
    def eval: Future[Option[Boolean]] = {
      val v = metric.value
      op match {
        case "="  => v.map(_.map(_ == value))
        case ">"  => v.map(_.map(_ >  value))
        case ">=" => v.map(_.map(_ >= value))
        case "<"  => v.map(_.map(_ >  value))
        case "<=" => v.map(_.map(_ >= value))
        case "!=" | "<>" => v.map(_.map(_ != value))

        case _ => Future(None)
      }
    }
  }

  final case class ContinuousAlarmRule(id: String, tid: String, markCount: Int)(implicit getter: AlarmValueGetter) extends RuleExpr with RuleEvaluation {
    def eval: Future[Option[Boolean]] = getter.continuousCount(tid).map(v => Some(v >= markCount))
  }

  final case class MetricRuleWindow(id: String, metric: Metric, value: Double, op: String, window: Long, agg: String) extends RuleExpr {
  }

  final case class WindowAlarmRule(id: String, rule: RuleExpr, window: Long) extends RuleExpr {
  }

  def main(args: Array[String]): Unit = {
    val f = interpret (
      MetricRule("1", Metric("server1", "cpu", Some(90)), 80, ">")
        && MetricRule("1", Metric("server1", "mem", Some(90)), 100, ">")
        || MetricRule("1", Metric("server1", "mem", Some(90)), 80, ">")
        //&& ContinuousAlarmRule("2", "1", 2)
    )
    f.foreach(println)

    Await.ready(f, Duration.Inf)

  }
}

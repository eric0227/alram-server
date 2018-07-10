package com.skt.tcore.model

import java.sql.Timestamp

import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import scala.concurrent.Future


trait Rule {
  val ruleId: String
}

trait RuleFilter {
  val condition: String
  def filter(f: String => Boolean): Future[Boolean] = async(f(condition))
}
case class StringFilter(str: String) extends RuleFilter {
  val condition: String = str
}

case class Metric(resource: String, metric: String, dc: Option[String]=None) extends RuleFilter {
  val condition = s"""(metric.resource = '${resource}' AND metric.metric = '${metric}')"""
}
case class MetricValue(resource: String, metric: String, dc: Option[String]=None, value: Double) extends RuleFilter {
  val condition = s"""(metric.resource = '${resource}' AND metric.metric = '${metric}')"""
  def condition(op: String) = s"""(metric.resource = '${resource}' AND metric.metric = '${metric}' AND  metric.value ${op} ${value})"""
}
object MetricValue {
  def apply(m:Metric, value: Double): MetricValue = new MetricValue(m.resource, m.metric, m.dc, value)
}

// stream => {'dc': 'dc1', resource: 'server1', 'metric': 'cpu', 'value'=85, 'timestamp': timestamp}
case class MetricRule(ruleId: String, resource: String, metric: String, value: Double, op: String, dc: Option[String] = None) extends Rule with RuleFilter {
  @Override
  val condition: String = s"""(metric.resource = '${resource}' AND metric.metric = '${metric}' AND  metric.value ${op} ${value})"""
  val metricCondition = s"""(metric.resource = '${resource}' AND metric.metric = '${metric}' IS NOT NULL)"""
  def eval(value: Double) : Boolean = op match {
    case "=" => value == value
    case ">" => value > value
    case ">=" => value >= value
    case "<" => value < value
    case "<=" => value <= value
    case "!=" | "<>" => value != value
    case _ => false
  }
}
object MetricRule {
  def apply(ruleId: String, mv: MetricValue, op: String): MetricRule = new MetricRule(ruleId, mv.resource, mv.metric, mv.value, op, mv.dc)
}

case class MetricRuleResult(rule: MetricRule, value: Double)

// metric_state_view : {'dc': 'dc1', resource: 'server1', 'metric': 'cpu', 'value'=85, 'timestamp': timestamp}
case class MetricStateRule(ruleId: String, resource: String, metric: String, value: Double, op: String, dc: Option[String]=None) extends Rule with RuleFilter {
  @Override
  val condition = s"""(metric_state_view.resource = '${resource}' AND metric_state_view.metric = '${metric}' AND  metric_state_view.value ${op} ${value})"""
  val metricCondition = s"""(metric_state_view.resource = '${resource}' AND metric_state_view.metric = '${metric}' IS NOT NULL)"""
}


// metric_rollup_view : {'dc': 'dc1', resource: 'server1', 'metric': 'cpu', 'value': {'cnt: 10, 'min': 50, 'mean': 65. 'max': 80, 'stddev': 23}, 'timestamp': 2018-07-23 11:01:00}
// metric_rollup_view : {'dc': 'dc1', resource: 'server1', 'metric': 'cpu', 'value': {'cnt: 12, 'min': 40, 'mean': 55. 'max': 80, 'stddev': 23}, 'timestamp': 2018-07-23 11:02:00}
case class MetricRollupRule(ruleId: String, resource: String, metric: String, summary: String, value: Double, op: String, dc: Option[String]=None) extends Rule with RuleFilter {
  @Override
  val condition = s"""(metric_rollup_view.resource = '${resource}' AND metric_rollup_view.metric = '${metric}' AND  metric_rollup_view.${summary} ${op} ${value})"""
  val metricCondition = s"""(metric_rollup_view.resource = '${resource}' AND metric_rollup_view.metric = '${metric}' IS NOT NULL)"""
}

// in => {'dc': 'dc1', resource: 'server1', 'line': '.....ERROR..', 'timestamp': timestamp}
case class LogRule(ruleId: String, resource: String, keyword: String, op: String, dc: Option[String]=None) extends Rule with RuleFilter {
  val condition = op match {
    case "=" => s"""log = '${keyword}'"""
    case "!=" => s"""log != '${keyword}'"""
    case "like" => s"""log like '%${keyword}%'"""
  }
}

// 지속 알람
case class ContinuousAlarmRule(ruleId: String, targetRuleId: String, markCount: Int, dc: Option[String]=None) extends Rule
case class ContinuousAlarmRuleAccumulator(continueRule: ContinuousAlarmRule, var occurCount: Int = 0, var occurTimestamp: Timestamp=new Timestamp(System.currentTimeMillis())) {
  def check(): Boolean = continueRule.markCount <= occurCount
  def increase() = this.occurCount = occurCount+1
  def reset() = this.occurCount = 0
}

// Window 알람
case class WindowAlarmRule(ruleId: String, windowFilter: RuleFilter, joinFilter:RuleFilter, markCount: Int=0, windowDuration: String = "1 minutes", slidingDuration: String = "1 seconds", dc: Option[String]=None) extends Rule

case class MetricLogic(logic: String="AND", opSeq: Seq[RuleFilter] = Seq())  extends RuleFilter {
  val condition = "(" + opSeq.foldLeft("")((s, b) => {
    val query = if(s == "")  b.condition else s + " " + logic + " " + b.condition
    query
  } + ")")

  def add(op: RuleFilter): MetricLogic = {
    this.copy(opSeq = opSeq :+ op)
  }
}
object MetricLogic {
  def apply(logic: String, op1: RuleFilter): MetricLogic = MetricLogic(logic, Seq(op1))
  def apply(logic: String, op1: RuleFilter, op2: RuleFilter): MetricLogic = MetricLogic(logic, Seq(op1, op2))
}



// test app
object MetricOpLogicTest extends App {

  println(
    MetricRule("r1", "server1", "cpu", 50, ">").condition
  )

  println(
    MetricLogic(
      "OR",
      MetricRule("r1","server1","cpu", 50, ">"),
      MetricRule("r1","server1","mem", 50, ">")
    ).condition
  )

  println(
    MetricLogic(
      "OR",
      MetricRule("r1","server1","cpu", 50, ">"),
      MetricRule("r1","server1","mem", 50, ">")
    ).add(
      MetricRule("r1","server2","disk", 100, ">")
    ).condition
  )
}
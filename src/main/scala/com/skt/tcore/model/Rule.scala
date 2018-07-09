package com.skt.tcore.model

import java.sql.Timestamp

import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import scala.concurrent.Future

trait Rule {
  val ruleId: String
  val dc: Option[String]
}

trait RuleFilter {
  val filterStr: String
  def filter(f: String => Boolean): Future[Boolean] = async {
      f(filterStr)
  }
}

case class StringFilter(str: String) extends RuleFilter {
  val filterStr: String = str
}

// stream => {'dc': 'dc1', resource: 'server1', 'metric': 'cpu', 'value'=85, 'timestamp': timestamp}
case class MetricRule(ruleId: String, resource: String, metric: String, value: Double, op: String, dc: Option[String]=None) extends Rule with RuleFilter {
  @Override
  val filterStr: String =s"""(metric.resource = '${resource}' AND metric.metric = '${metric}' AND  metric.value ${op} ${value})"""
  def metricNameFilter = s"""(metric.resource = '${resource}' AND metric.metric = '${metric}' IS NOT NULL)"""

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
case class MetricRuleResult(rule: MetricRule, value: Double)

// metric_state_view : {'dc': 'dc1', resource: 'server1', 'metric': 'cpu', 'value'=85, 'timestamp': timestamp}
case class MetricStateRule(ruleId: String, resource: String, metric: String, value: Double, op: String, dc: Option[String]=None) extends Rule with RuleFilter {
  @Override
  val filterStr: String =s"""(metric_state_view.resource = '${resource}' AND metric_state_view.metric = '${metric}' AND  metric_state_view.value ${op} ${value})"""
  def metricNameFilter() = s"""(metric_state_view.resource = '${resource}' AND metric_state_view.metric = '${metric}' IS NOT NULL)"""
}


// metric_rollup_view : {'dc': 'dc1', resource: 'server1', 'metric': 'cpu', 'value': {'cnt: 10, 'min': 50, 'mean': 65. 'max': 80, 'stddev': 23}, 'timestamp': 2018-07-23 11:01:00}
// metric_rollup_view : {'dc': 'dc1', resource: 'server1', 'metric': 'cpu', 'value': {'cnt: 12, 'min': 40, 'mean': 55. 'max': 80, 'stddev': 23}, 'timestamp': 2018-07-23 11:02:00}
case class MetricRollupRule(ruleId: String, resource: String, metric: String, summary: String, value: Double, op: String, dc: Option[String]=None) extends Rule with RuleFilter {
  @Override
  val filterStr: String =s"""(metric_rollup_view.resource = '${resource}' AND metric_rollup_view.metric = '${metric}' AND  metric_rollup_view.${summary} ${op} ${value})"""
  def metricNameFilter() = s"""(metric_rollup_view.resource = '${resource}' AND metric_rollup_view.metric = '${metric}' IS NOT NULL)"""
}

// in => {'dc': 'dc1', resource: 'server1', 'line': '.....ERROR..', 'timestamp': timestamp}
case class LogRule(ruleId: String, resource: String, keyword: String, op: String, dc: Option[String]=None) extends Rule with RuleFilter {
  val filterStr = op match {
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
  val filterStr = "(" + opSeq.foldLeft("")((s, b) => {
    val query = if(s == "")  b.filterStr else s + " " + logic + " " + b.filterStr
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
    MetricRule("r1", "server1", "cpu", 50, ">").filterStr
  )

  println(
    MetricLogic(
      "OR",
      MetricRule("r1","server1","cpu", 50, ">"),
      MetricRule("r1","server1","mem", 50, ">")
    ).filterStr
  )

  println(
    MetricLogic(
      "OR",
      MetricRule("r1","server1","cpu", 50, ">"),
      MetricRule("r1","server1","mem", 50, ">")
    ).add(
      MetricRule("r1","server2","disk", 100, ">")
    ).filterStr
  )
}
package com.skt.tcore.model

import java.sql.Timestamp

//
//case class Metric(name: String) {
//  def filter(): String = s"metric.${name} IS NOT NULL"
//  def rule(v: Double, op: String): MetricRule =  MetricRule(this, v, op)
//}

trait Rule {
  val ruleId: String
}

trait RuleFilter {
  def filter(): String
}
case class StringFilter(str: String) extends RuleFilter {
  def filter(): String = str
}
case class MetricRule(ruleId: String, name: String, v: Double, op: String) extends Rule with RuleFilter {
  def filter() = s"""(metric.${name} IS NOT NULL AND metric.${name} ${op} ${v})"""
  def eval(value: Double) : Boolean = op match {
    case "=" => value == v
    case ">" => value > v
    case ">=" => value >= v
    case "<" => value < v
    case "<=" => value <= v
    case "!=" | "<>" => value != v
    case _ => false
  }
}

case class EventRule(ruleId: String, resource: String, name: String, value: Double, op: String) extends Rule with RuleFilter {
  def metricNameFilter() = s"""(resource = '${resource}' AND metric.${name} IS NOT NULL)"""
  def filter() = s"""(resource = '${resource}' AND metric.${name} IS NOT NULL AND metric.${name} ${op} ${value})"""
  def keyFilter() = s"""(resource = '${resource}' AND key = '${name}')"""
  def keyValueFilter() = s"""(resource = '${resource}' AND key = '${name}' AND value ${op} ${value})"""
}

case class ResourceRule(resource: String, logic: String="AND", mSeq: Seq[MetricRule] = Seq()) extends RuleFilter {
  def filter() = {
    val metricFilter = "(" + mSeq.foldLeft("")((s, b) => {
      if(s == "")  b.filter() else s + " " + logic + " " + b.filter()
    }) + ")"
    s"""(resource = '${resource}' AND ${metricFilter}"""
  }
  def add(m: MetricRule): ResourceRule = {
    this.copy(mSeq = mSeq :+ m)
  }
}

case class MetricLogic(logic: String="AND", opSeq: Seq[RuleFilter] = Seq())  extends RuleFilter {
  def filter() = "(" + opSeq.foldLeft("")((s, b) => {
    val query = if(s == "")  b.filter() else s + " " + logic + " " + b.filter()
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

case class ContinuousAlarmRule(ruleId: String, targetRuleId: String, markCount: Int) extends Rule

case class ContinuousAlarmRuleAccumulator(continueRule: ContinuousAlarmRule, var occurCount: Int = 0, var occurTimestamp: Timestamp=new Timestamp(System.currentTimeMillis())) {
  def check(): Boolean = continueRule.markCount <= occurCount
  def increase() = this.occurCount = occurCount+1
  def reset() = this.occurCount = 0
}


// test app
object MetricOpLogicTest extends App {
  println(
    MetricRule("r1", "cpu", 50, ">").filter
  )

  println(
    MetricLogic(
      "AND",
      MetricRule("r1","cpu", 50, ">"),
      MetricRule("r1","mem", 50, ">")
    ).filter()
  )

  println(
    MetricLogic(
      "AND",
      MetricRule("r1","cpu", 50, ">"),
      MetricRule("r1","mem", 50, ">")
    ).add(
      MetricRule("r1","disk", 100, ">")
    ).filter()
  )

  println(
    MetricLogic(
      "OR",
      ResourceRule("resource1", "AND")
        .add(MetricRule("r1","cpu", 50, ">"))
        .add(MetricRule("r1","mem", 60, ">")),
      ResourceRule("resource2", "AND")
        .add(MetricRule("r1","cpu", 50, ">"))
        .add(MetricRule("r1","mem", 60, ">"))
    ).add(
      ResourceRule("resource3", "AND")
        .add(MetricRule("r1","cpu", 50, ">"))
        .add(MetricRule("r1","mem", 60, ">"))
    )
  )
}
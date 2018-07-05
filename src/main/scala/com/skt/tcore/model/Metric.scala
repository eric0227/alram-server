package com.skt.tcore.model

//
//case class Metric(name: String) {
//  def filter(): String = s"metric.${name} IS NOT NULL"
//  def rule(v: Double, op: String): MetricRule =  MetricRule(this, v, op)
//}


trait MetricFilter {
  def filter(): String
}
case class StringFilter(str: String) extends MetricFilter {
  def filter(): String = str
}
case class MetricRule(name: String, v: Double, op: String) extends MetricFilter {
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

case class EventRule(ruleId: String, resource: String, name: String, v: Double, op: String) extends MetricFilter {
  def metricNameFilter() = s"""(resource = '${resource}' AND metric.${name} IS NOT NULL)"""
  def filter() = s"""(resource = '${resource}' AND metric.${name} IS NOT NULL AND metric.${name} ${op} ${v})"""
  def keyValueFilter() = s"""(resource = '${resource}' AND key = '${name}'  AND value ${op} ${v})"""
}

case class ResourceRule(resource: String, logic: String="AND", mSeq: Seq[MetricRule] = Seq()) extends MetricFilter {
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

case class MetricLogic(logic: String="AND", opSeq: Seq[MetricFilter] = Seq())  extends MetricFilter {
  def filter() = "(" + opSeq.foldLeft("")((s, b) => {
    val query = if(s == "")  b.filter() else s + " " + logic + " " + b.filter()
    query
  } + ")")

  def add(op: MetricFilter): MetricLogic = {
    this.copy(opSeq = opSeq :+ op)
  }
}
object MetricLogic {
  def apply(logic: String, op1: MetricFilter): MetricLogic = MetricLogic(logic, Seq(op1))
  def apply(logic: String, op1: MetricFilter, op2: MetricFilter): MetricLogic = MetricLogic(logic, Seq(op1, op2))
}


// test app
object MetricOpLogicTest extends App {
  println(
    MetricRule( "cpu", 50, ">").filter
  )

  println(
    MetricLogic(
      "AND",
      MetricRule("cpu", 50, ">"),
      MetricRule("mem", 50, ">")
    ).filter()
  )

  println(
    MetricLogic(
      "AND",
      MetricRule("cpu", 50, ">"),
      MetricRule("mem", 50, ">")
    ).add(
      MetricRule("disk", 100, ">")
    ).filter()
  )

  println(
    MetricLogic(
      "OR",
      ResourceRule("resource1", "AND")
        .add(MetricRule("cpu", 50, ">"))
        .add(MetricRule("mem", 60, ">")),
      ResourceRule("resource2", "AND")
        .add(MetricRule("cpu", 50, ">"))
        .add(MetricRule("mem", 60, ">"))
    ).add(
      ResourceRule("resource3", "AND")
        .add(MetricRule("cpu", 50, ">"))
        .add(MetricRule("mem", 60, ">"))
    )
  )
}
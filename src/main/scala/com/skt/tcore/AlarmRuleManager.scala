package com.skt.tcore

import com.skt.tcore.model._

object AlarmRuleManager {

  var ruleList = Seq[Rule]()

  def addRule(rule: Rule): Unit = {
    if(ruleList.exists(_.ruleId == rule.ruleId)) {
      new IllegalArgumentException(s"duplicated rule id:"+rule.ruleId)
    }
    ruleList = ruleList :+ rule
  }

  def clear(): Unit = {
    ruleList = Seq[Rule]()
  }

  def removeEventRule(id: String): Unit = {
    ruleList = ruleList.dropWhile(_.ruleId == id)
  }

  def updateEventRule(rule: Rule): Unit = {
    ruleList = ruleList.dropWhile(_.ruleId == rule.ruleId)
    addRule(rule)
  }

  def getMetricRule(): Seq[MetricRule] = {
    ruleList.filter {
      case r: MetricRule => true
      case _ => false
    }.map(_.asInstanceOf[MetricRule])
  }

  def getContinuousAlarmRule(): Seq[ContinuousAlarmRule] = {
    ruleList.filter {
      case r: ContinuousAlarmRule => true
      case _ => false
    }.map(_.asInstanceOf[ContinuousAlarmRule])
  }

  def createDummyRule() : Seq[Rule] = synchronized {
    if(ruleList.isEmpty) {
      AlarmRuleManager.addRule(MetricRule("r1", "server1", "cpu", 80, ">"))
      AlarmRuleManager.addRule(MetricRule("r2", "server1", "mem", 90, ">"))

      AlarmRuleManager.addRule(ContinuousAlarmRule("c1", "r1", 3))
      AlarmRuleManager.addRule(ContinuousAlarmRule("c2", "r2", 3))
    }
    ruleList
  }

}

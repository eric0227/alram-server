package com.skt.tcore

import com.skt.tcore.model.{EventRule, MetricRule, ResourceRule}

object EventDetectManager {

  var eventRuleList: Seq[EventRule] = Seq()

  def addEventRule(rule: EventRule): Unit = {
    if(eventRuleList.exists(_.ruleId == rule.ruleId)) {
      new IllegalArgumentException(s"duplicated rule id:"+rule.ruleId)
    }
    eventRuleList = eventRuleList :+ rule
  }

//  def addEventRule(id: String)(condition: String): Unit = {
//    eventRuleList = eventRuleList :+ EventRule(id, StringCondition(condition))
//  }

  def removeEventRule(id: String): Unit = {
    eventRuleList = eventRuleList.dropWhile(_.ruleId == id)
  }

  def updateEventRule(rule: EventRule): Unit = {
    eventRuleList = eventRuleList.dropWhile(_.ruleId == rule.ruleId)
    addEventRule(rule)
  }

  def getEventRule(): Seq[EventRule] = {
    eventRuleList
  }

  def createDummyEventRule() : Seq[EventRule] = {
    EventDetectManager.addEventRule(EventRule("r1", "server1", "cpu", 80, ">"))
    EventDetectManager.addEventRule(EventRule("r2", "server1", "mem", 90, ">"))
    eventRuleList
  }
}

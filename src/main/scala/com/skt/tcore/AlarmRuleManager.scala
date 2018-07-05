package com.skt.tcore

import com.skt.tcore.model._

object AlarmRuleManager {

  var ruleList = Seq[Rule]()

  def addEventRule(rule: Rule): Unit = {
    if(ruleList.exists(_.ruleId == rule.ruleId)) {
      new IllegalArgumentException(s"duplicated rule id:"+rule.ruleId)
    }
    ruleList = ruleList :+ rule
  }

  def removeEventRule(id: String): Unit = {
    ruleList = ruleList.dropWhile(_.ruleId == id)
  }

  def updateEventRule(rule: Rule): Unit = {
    ruleList = ruleList.dropWhile(_.ruleId == rule.ruleId)
    addEventRule(rule)
  }

  def getEventRule(): Seq[EventRule] = {
    ruleList.filter {
      case r: EventRule => true
      case _ => false
    }.map(_.asInstanceOf[EventRule])
  }

  def getContinuousAlarmRule(): Seq[ContinuousAlarmRule] = {
    ruleList.filter {
      case r: ContinuousAlarmRule => true
      case _ => false
    }.map(_.asInstanceOf[ContinuousAlarmRule])
  }

  def createDummyRule() : Seq[Rule] = synchronized {
    if(ruleList.isEmpty) {
      AlarmRuleManager.addEventRule(EventRule("r1", "server1", "cpu", 80, ">"))
      AlarmRuleManager.addEventRule(EventRule("r2", "server1", "mem", 90, ">"))

      AlarmRuleManager.addEventRule(ContinuousAlarmRule("c1", "r1", 3))
      AlarmRuleManager.addEventRule(ContinuousAlarmRule("c2", "r2", 3))
    }
    ruleList
  }

}

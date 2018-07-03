package com.skt.tcore.model

import java.sql.Timestamp

case class EventRuleResult(ruleId: String, resource: String, name: String,  v: Double, op: String,  timestamp: Timestamp, detect: Int, filter: String)

object EventRuleResult {
  def apply(rule: EventRule,  timestamp: Timestamp, detect: Int, filter: String): EventRuleResult = {
    EventRuleResult(rule.ruleId, rule.resource, rule.name, rule.v, rule.op, timestamp, detect, filter)
  }
}

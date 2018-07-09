package com.skt.tcore

import com.skt.tcore.model.{ContinuousAlarmRule, MetricRule}
import org.scalatest.FunSuite

class AlarmRuleManagerTest extends FunSuite{

  test("EventDetectManager") {
    AlarmRuleManager.addRule(MetricRule("r1", "server1", "cpu", 80, ">"))
    AlarmRuleManager.addRule(MetricRule("r2", "server1", "mem", 90, ">"))
    AlarmRuleManager.addRule(ContinuousAlarmRule("c1", "r1", 3))
    AlarmRuleManager.addRule(ContinuousAlarmRule("c2", "r2", 3))

    assert( AlarmRuleManager.ruleList.size == 4)
    AlarmRuleManager.ruleList.foreach(println)

    AlarmRuleManager.clear()
    assert( AlarmRuleManager.ruleList.isEmpty)
  }
}

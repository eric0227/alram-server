package com.skt.tcore

import org.scalatest.FunSuite

class AlarmRuleManagerTest extends FunSuite{

  test("EventDetectManager") {
    val list = AlarmRuleManager.createDummyEventRule()
    list.foreach(r => println(r))
  }
}

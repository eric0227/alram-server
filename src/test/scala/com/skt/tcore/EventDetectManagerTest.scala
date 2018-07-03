package com.skt.tcore

import org.scalatest.FunSuite

class EventDetectManagerTest extends FunSuite{

  test("EventDetectManager") {
    val list = EventDetectManager.createDummyEventRule()
    list.foreach(r => println(r))
  }
}

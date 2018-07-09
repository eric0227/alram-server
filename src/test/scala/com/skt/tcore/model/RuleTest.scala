package com.skt.tcore.model

import org.scalatest.FunSuite

class RuleTest extends FunSuite {

  test("MetricRule filterStr") {
    assert(
      MetricRule("r1", "server1", "cpu", 50, ">").filterStr
        == "(metric.resource = 'server1' AND metric.metric = 'cpu' AND  metric.value > 50.0)"
    )
  }

  test("MetricLogic filterStr") {
    assert(
      MetricLogic(
        "OR",
        MetricRule("r1", "server1", "cpu", 50, ">"),
        MetricRule("r1", "server1", "mem", 50, ">")
      ).filterStr
        == "((metric.resource = 'server1' AND metric.metric = 'cpu' AND  metric.value > 50.0)) OR (metric.resource = 'server1' AND metric.metric = 'mem' AND  metric.value > 50.0))"
    )

    assert(
      MetricLogic(
        "OR",
        MetricRule("r1", "server1", "cpu", 50, ">"),
        MetricRule("r1", "server1", "mem", 50, ">")
      ).add(
        MetricRule("r1", "server2", "disk", 100, ">")
      ).filterStr
        == "((metric.resource = 'server1' AND metric.metric = 'cpu' AND  metric.value > 50.0)) OR (metric.resource = 'server1' AND metric.metric = 'mem' AND  metric.value > 50.0)) OR (metric.resource = 'server2' AND metric.metric = 'disk' AND  metric.value > 100.0))"
    )
  }

  test("MetricStateRule filterStr") {
    assert (
      MetricStateRule("r1", "server1", "cpu", 50, ">").filterStr
        == "(metric_state_view.resource = 'server1' AND metric_state_view.metric = 'cpu' AND  metric_state_view.value > 50.0)"
    )
  }

  test("MetricRollupRule filterStr") {
    assert (
      MetricRollupRule("r1", "server1", "cpu", "mean", 50, ">").filterStr
        == "(metric_rollup_view.resource = 'server1' AND metric_rollup_view.metric = 'cpu' AND  metric_rollup_view.mean > 50.0)"
    )
  }

  test("MetricLogic AND  MetricStateRule") {
    assert (
      MetricLogic(
        "AND",
        MetricRule("r1", "server1", "cpu", 80, ">"),
        MetricStateRule("r1", "server1", "mem", 75, ">")
      ).filterStr
        == "((metric.resource = 'server1' AND metric.metric = 'cpu' AND  metric.value > 80.0)) AND (metric_state_view.resource = 'server1' AND metric_state_view.metric = 'mem' AND  metric_state_view.value > 75.0))"
    )
  }

  test("MetricLogic AND  MetricRollupRule") {
    assert (
      MetricLogic(
        "AND",
        MetricRule("r1", "server1", "cpu", 80, ">"),
        MetricRollupRule("r1", "server1", "mem", "mean", 75, ">")
      ).filterStr
        == "((metric.resource = 'server1' AND metric.metric = 'cpu' AND  metric.value > 80.0)) AND (metric_rollup_view.resource = 'server1' AND metric_rollup_view.metric = 'mem' AND  metric_rollup_view.mean > 75.0))"
    )
  }

  test("LogRule filterStr") {
    assert(
      LogRule("r1", "server1", "ERROR", "=").filterStr
        == "log = 'ERROR'"
    )
    assert(
      LogRule("r1", "server1", "ERROR", "!=").filterStr
        == "log != 'ERROR'"
    )
    assert(
      LogRule("r1", "server1", "ERROR", "like").filterStr
        == "log like '%ERROR%'"
    )
  }
}

package com.skt.tcore

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.skt.tcore.model.{MetricRule, MetricLogic}
import test.SparkSessionTestWrapper
import org.scalatest.FunSuite

class CepEngineTest extends FunSuite  with SparkSessionTestWrapper with DatasetComparer {
  import spark.implicits._

  test("CepEngine") {
    // cep
    val cepEngine = new CepEngine(spark)
    cepEngine.executeAlarmRule("rule1",
      MetricLogic(logic = "OR")
        .add(MetricRule("r1", "server1", "cpu", 20, ">"))
        .add(MetricRule("r2", "server1", "mem", 80, ">"))) { df =>
      df.show(numRows = 100)
    }
  }
}

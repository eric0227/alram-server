package stresstest

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

case class MetricRule(resource: String, metric: String, value: Double, op: String = ">") {
  def rkey(prefix: String) = prefix+":"+resource
  def rfield = metric
  def eval(v: Double) : Boolean = op match {
    case "=" =>  v == value
    case ">" =>  v >  value
    case ">=" => v >= value
    case "<" =>  v <  value
    case "<=" => v <= value
    case "!=" | "<>" => v != value
    case _ => false
  }
}


object MetricRule {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def apply(json: String): MetricRule = {
    val rule = mapper.readValue[MetricRule](json, classOf[MetricRule])
    rule
  }
}

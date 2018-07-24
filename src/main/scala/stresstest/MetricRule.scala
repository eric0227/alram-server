package stresstest

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

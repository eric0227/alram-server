package test

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.functions.udf
import org.scalatest.FunSuite

class DynamicFilterTest extends FunSuite with SparkSessionTestWrapper with DatasetComparer {

  test("DynamicFilterTest") {
    import spark.implicits._

    case class AlarmRule(id: String, event: String, metric: Double, op: String) {
      def eval(value: Double) : Boolean = op match {
        case "=" => metric == value
        case ">" => metric > value
        case ">=" => metric >= value
        case "<" => metric < value
        case "<=" => metric <= value
        case "!=" | "<>" => metric != value
        case _ => false
      }
    }
    val userFilter = (resource: String, event: String, metric: Double) => {
      val ruleList = List(AlarmRule("1", "cpu", 90, ">")) // 동적으로 로딩 함.
      ruleList.map (r => if(event == r.event && r.eval(metric)) 1 else 0).sum > 0
    }
    def dynamicFilter = udf(userFilter)

//    streamDf
//      .filter(dynamicFilter($"resource", $"event", $"metric") === true)
//      .writeStream
//      .format("console")
//      .option("header", "true").option("truncate", false)
//      .start()
  }
}

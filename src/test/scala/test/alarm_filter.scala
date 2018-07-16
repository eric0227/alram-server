package test

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import test.alarm_detect.{AlarmRule, alarmRuleDf, selectQuery}

object alarm_filter {

  def main(args: Array[String]) = {

    case class AlarmRule(id: String, event: String, metric: Double, op: String) extends Serializable {
      def eval(value: Double) : Boolean = op match {
        case "=" =>  value == metric
        case ">" =>  value >  metric
        case ">=" => value >= metric
        case "<" =>  value <  metric
        case "<=" => value <= metric
        case "!=" | "<>" => metric != value
        case _ => false
      }
    }
    implicit val alarmRuleEncoder = org.apache.spark.sql.Encoders.kryo[AlarmRule]

    case class AlarmResult(id: String="", nodegroup: String="", resource:String="", event: String, metric: Double, chk:Int=0, log: String="") extends Serializable
    implicit val alarmRuleResultEncoder = org.apache.spark.sql.Encoders.kryo[AlarmResult]

    val data =
    """
      | {"nodegroup":"g1","resource": "r1","event":"sys.cpu.usage", "metric":90, "log":""}
      | {"nodegroup":"g1","resource": "r1","event":"sys.cpu.usage", "metric":10, "log":""}
      | {"nodegroup":"g1","resource": "r1","event":"sys.memory.usage", "metric":90, "log":""}
      | {"nodegroup":"g1","resource": "r1","event":"sys.disk.usage", "metric":90, "log":""}
      |
    """

    import org.apache.spark.sql.SparkSession
    val ss = SparkSession
      .builder()
      .appName("AlertStructuredStreaming")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 10)
      .getOrCreate()

    import ss.implicits._

    val kafkaDf = ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", args(0))
      .option("subscribe", "cep-streaming")
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    val schema = StructType(Seq(
      StructField("nodegroup", StringType, true),
      StructField("metric", DoubleType, true),
      StructField("resource", StringType, true),
      StructField("event", StringType, true),
      StructField("log", StringType, true)
    ))

    // Driver 프로그램에서 호출
    var alarmRuleBc: Broadcast[List[AlarmRule]] = null
    def createOrReplaceAlarmRule(ruleList: List[AlarmRule]) = {
      alarmRuleBc = ss.sparkContext.broadcast(ruleList)
    }
    //var alarmRuleDf: DataFrame = null
    def createOrReplaceAlarmRule2(ruleList: List[AlarmRule]) = {
      val df = ruleList.toDF
      //if (alarmRuleDf != null) alarmRuleDf.unpersist()
      df.cache()
      df.createOrReplaceTempView("rule")
      df.show()
      df
    }

    createOrReplaceAlarmRule(List(AlarmRule("1", "sys.cpu.usage", 80, ">"), AlarmRule("2", "sys.memory.usage", 90, ">"), AlarmRule("3", "sys.disk.usage", 90, ">")))
    var alarmRuleDf: DataFrame = createOrReplaceAlarmRule2(List(AlarmRule("1", "sys.cpu.usage", 80, ">"), AlarmRule("2", "sys.memory.usage", 90, ">"), AlarmRule("3", "sys.disk.usage", 90, ">")))



    val selectQuery = kafkaDf
      .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")
      .as[(String, String)]
      .select(from_json($"value", schema = schema).as("data"))
      .selectExpr(
        "data.resource as resource", "data.nodegroup as nodegroup", "data.event as event",
        "CAST(data.metric AS DOUBLE) as metric",
        "data.log as log").select("nodegroup", "event", "metric", "log").toDF

    // RDD Operation
    //implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    val join = selectQuery.mapPartitions{ iter =>
      if(iter.isEmpty) Seq[String]().iterator // Seq[AlarmResult]().iterator
      else {

        //alarmRuleDf.show()
        ss.sql("select * from rule").show()

        val ruleList: List[AlarmRule] = alarmRuleBc.value
        println(ruleList)
        iter.map { row =>
          val event = row.getAs[String]("event")
          val metric = row.getAs[Double]("metric")

          val r = ruleList.find { rule =>
            rule.event == event && rule.eval(metric)
          }
          //row.getValuesMap[Any](List("nodegroup", "event", "metric", "log")) ++ Map("id" -> r.map(_.id).getOrElse(""), "chk" -> r.map(_ => 1).getOrElse(0))
          val id = r.map(_.id).getOrElse("")
          val chk = r.map(_ => 1).getOrElse(0)
          //AlarmResult(id = id, event = event, metric = metric, chk = chk)
          s"""{"id" = "$id", "event" = "$event", "metric" = $metric, "chk" = $chk}"""
        }
      }
    }

    join.writeStream.format("console").option("truncate", false).start()

//    val finalQuery = join.select(to_json(struct($"id",$"nodegroup", $"event", $"log", $"metric")).as("value"))
//    val executingQuery = finalQuery
//      .writeStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", args(0))
//      .option("topic", "event-streaming")
//      .option("checkpointLocation", "/tmp/spark-streaming")
//      .outputMode(OutputMode.Append())
//      .start()

    createOrReplaceAlarmRule(List(AlarmRule("1", "sys.cpu.usage", 50, ">"), AlarmRule("2", "sys.memory.usage", 50, ">"), AlarmRule("3", "sys.disk.usage", 50, ">")))
    ss.streams.awaitAnyTermination()

  }
}

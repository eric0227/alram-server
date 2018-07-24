package stresstest

import com.skt.tcore.common.Common
import com.skt.tcore.common.Common.maxOffsetsPerTrigger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import stresstest.AlarmDetectionStressBroadcastUDFTest.spark

object AlarmRuleRedisLoaderBroadcast extends App {

  val master = if (args.length == 1) Some(args(0)) else None
  val builder = SparkSession.builder().appName("spark test")
  master.foreach(mst => builder.master(mst))
  implicit val spark = builder.getOrCreate()

  val options = scala.collection.mutable.HashMap[String, String]()
  maxOffsetsPerTrigger.foreach(max => options += ("maxOffsetsPerTrigger" -> max.toString))

  var alarmRuleBc: Broadcast[List[MetricRule]] = _
  def createBroadcast(ruleList: List[MetricRule]) = {
    val backup = alarmRuleBc
    alarmRuleBc = spark.sparkContext.broadcast(ruleList)
    if(backup != null) backup.destroy()
  }

  AlarmRuleRedisLoader { list =>
    println(list.size)
    Common.watchTime("create Broadcast") {
      createBroadcast(list.toList)
    }
  }.loadRedisRule()

  spark.streams.awaitAnyTermination()
}

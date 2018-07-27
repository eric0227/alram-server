package stresstest

import com.skt.tcore.common.Common
import com.skt.tcore.common.Common.maxOffsetsPerTrigger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

object AlarmRuleRedisLoaderBroadcast extends App {

  val master = if (args.length == 1) Some(args(0)) else None
  val builder = SparkSession.builder().appName("AlarmRuleRedisLoaderBroadcast")
  master.foreach(mst => builder.master(mst))
  implicit val spark = builder.getOrCreate()

  val initSeq = (1 to 1000).map(n => (n,n))
  val rdd = spark.sparkContext.parallelize(initSeq, 20)
  rdd.reduceByKey(_ + _).count()
  println("start application..")

  var alarmRuleBc: Broadcast[List[MetricRule]] = _
  def createBroadcast(ruleList: List[MetricRule]) = {
    val backup = alarmRuleBc
    alarmRuleBc = spark.sparkContext.broadcast(ruleList)
    if(backup != null) backup.destroy()
    ruleList.groupBy(_.metric).foreach(d => println(d._1, d._2.size))
  }

  AlarmRuleRedisLoader { list =>
    createBroadcast(list.toList)
  }

  spark.streams.awaitAnyTermination()
}

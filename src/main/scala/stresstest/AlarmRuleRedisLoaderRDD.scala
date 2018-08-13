package stresstest

import com.skt.tcore.common.Common
import com.skt.tcore.common.Common.maxOffsetsPerTrigger
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import stresstest.AlarmDetectionStressRddJoinTest.{ruleDf, spark}

object AlarmRuleRedisLoaderRDD extends App {

  val master = if (args.length == 1) Some(args(0)) else None
  val builder = SparkSession.builder().appName("AlarmRuleRedisLoaderRDD")
  master.foreach(mst => builder.master(mst))
  implicit val spark = builder.getOrCreate()
  import spark.implicits._
  import org.apache.spark.sql._

  println("start application..")

  var ruleDf: DataFrame = _
  def createRuleDF(ruleList: List[MetricRule]) = {

    if(ruleDf != null) ruleDf.unpersist(true)
    ruleDf = broadcast(spark.sqlContext.createDataFrame(ruleList)).toDF().cache()
    ruleDf.createOrReplaceTempView("metric_rule")

    println("create dataframe ..ok")
  }

  AlarmRuleRedisLoader { list =>
    createRuleDF(list.toList)
  }

  spark.streams.awaitAnyTermination()
}

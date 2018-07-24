package stresstest

import com.skt.tcore.common.Common
import com.skt.tcore.common.Common.maxOffsetsPerTrigger
import org.apache.spark.sql.{DataFrame, SparkSession}

object AlarmRuleRedisLoaderRDD extends App {

  val master = if (args.length == 1) Some(args(0)) else None
  val builder = SparkSession.builder().appName("spark test")
  master.foreach(mst => builder.master(mst))
  implicit val spark = builder.getOrCreate()

  val options = scala.collection.mutable.HashMap[String, String]()
  maxOffsetsPerTrigger.foreach(max => options += ("maxOffsetsPerTrigger" -> max.toString))

  var ruleDf: DataFrame = _
  def createRuleDF(ruleList: List[MetricRule]) = synchronized {
    val df: DataFrame = spark.sqlContext.createDataFrame(ruleList)
    df.repartition(df("resource")).cache().createOrReplaceTempView("metric_rule")
    if (ruleDf != null) ruleDf.unpersist()
    ruleDf = df
    spark.sql("select * from metric_rule").show(truncate = false)
  }

  AlarmRuleRedisLoader { list =>
    println(list.size)
    Common.watchTime("create Rule RDD") {
      createRuleDF(list.toList)
    }
  }.loadRedisRule()

  spark.streams.awaitAnyTermination()
}

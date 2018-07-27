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

//  val initSeq = (1 to 1000).map(n => (n,n))
//  val rdd = spark.sparkContext.parallelize(initSeq, 20)
//  rdd.reduceByKey(_ + _).count()
  println("start application..")

  var ruleDf: DataFrame = _
  def createRuleDF(ruleList: List[MetricRule]) = {
/*
    val df: DataFrame = spark.sqlContext.createDataFrame(ruleList)
    df.repartition(20, df("resource")).cache().createOrReplaceTempView("metric_rule")
    if (ruleDf != null) ruleDf.unpersist()
    ruleDf = df
    spark.sql("select metric, count(*) from metric_rule group by metric").show(truncate = false)
*/
//    if(ruleDf != null) ruleDf.unpersist(true)
//    val df: DataFrame = spark.sqlContext.createDataFrame(ruleList)
//    df.repartition(20, df("resource"), df("metric")).cache().createOrReplaceTempView("metric_rule")
//    ruleDf = df

    if(ruleDf != null) ruleDf.unpersist(true)
    ruleDf = broadcast(spark.sqlContext.createDataFrame(ruleList)).toDF().cache()
    ruleDf.createOrReplaceTempView("metric_rule")

    //spark.sql("select metric, count(*) from metric_rule group by metric").show(truncate = false)
    //spark.sql("select count(*) from metric_rule").show(truncate = false)
    println("create dataframe ..ok")
  }

  AlarmRuleRedisLoader { list =>
    createRuleDF(list.toList)
  }

  spark.streams.awaitAnyTermination()
}

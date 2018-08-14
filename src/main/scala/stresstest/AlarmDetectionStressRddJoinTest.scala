package stresstest

import com.skt.tcore.AlarmServer
import com.skt.tcore.common.Common.{checkpointPath, kafkaServers, maxOffsetsPerTrigger, metricTopic}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object AlarmDetectionStressRddJoinTest extends App {

  val master = if (args.length == 1) Some(args(0)) else None
  val builder = SparkSession.builder().appName("AlarmDetectionStressRddJoinTest")
  master.foreach(mst => builder.master(mst))
  builder.config("spark.sql.streaming.checkpointLocation",  checkpointPath+"/SparkSessionTest")

  implicit val spark = builder.getOrCreate()

  import spark.implicits._

  val options = scala.collection.mutable.HashMap[String, String]()
  maxOffsetsPerTrigger.foreach(max => options += ("maxOffsetsPerTrigger" -> max.toString))

  val eventStreamDF = AlarmServer.readKafkaDF(kafkaServers, metricTopic, options.toMap)(spark)
  eventStreamDF.printSchema()
  val streamDf = AlarmServer.selectMetricEventDF(eventStreamDF)
  streamDf.printSchema()

  val initSeq = (1 to 1000).map(n => (n,n))
  val rdd = spark.sparkContext.parallelize(initSeq, 20)
  rdd.reduceByKey(_ + _).count()
  println("start application..")

  var ruleDf: DataFrame = _
  var query: StreamingQuery = _
  def createRuleDF(ruleList: List[MetricRule]) = {
    val _df = ruleDf
    ruleDf = broadcast(spark.sqlContext.createDataFrame(ruleList)).toDF()
    ruleDf.createOrReplaceTempView("metric_rule")
    if(query != null) query.stop()
    if(_df != null) _df.unpersist()
    query = startQuery()
    println("create dataframe ..ok")
  }

  AlarmRuleRedisLoader { list =>
    createRuleDF(list.toList)
  }.loadRedisRule()

  def startQuery(): StreamingQuery = synchronized {

    val join = spark.sql(
      """
        | select m.timestamp, m.resource, m.metric,
        |        case when r.op = '='  and m.value =  r.value then 1
        |             when r.op = '>'  and m.value >  r.value then 1
        |             when r.op = '>=' and m.value >= r.value then 1
        |             when r.op = '<'  and m.value <  r.value then 1
        |             when r.op = '<=' and m.value <= r.value then 1
        |             when r.op = '!=' and m.value != r.value then 1
        |             when r.op = '<>' and m.value <> r.value then 1
        |        else 0 end chk
        | from metric m
        | inner join metric_rule r
        | on m.resource = r.resource and m.metric = r.metric
      """.stripMargin)
      .mapPartitions { iter =>
        iter.toList.map { r =>
          (r.getAs[String]("metric"), r.getAs[Int]("chk"), 1)
        }.groupBy(d => (d._1,d._2)).map(d => (d._1._1, d._1._2, d._2.size)).iterator
      }
    join.writeStream
      .format("stresstest.CountSinkProvider")
      .trigger(Trigger.ProcessingTime(0))
      .option("checkpointLocation", checkpointPath + "/rdd")
      .start()
  }
  //startQuery()

  while(true) {
    spark.streams.awaitAnyTermination()
    Thread.sleep(1000)
  }
}

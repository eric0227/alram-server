package sink

import com.skt.tcore.AlarmRuleManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.functions._

class EventDetectSink(options: Map[String, String]) extends Sink with Logging {
  val bootstrap = options.get("kafka.bootstrap.servers").get
  val topic = options.get("topic").get

  override def addBatch(batchId: Long, data: DataFrame): Unit = synchronized {
    import data.sparkSession.implicits._
    // test
    AlarmRuleManager.createDummyRule()
    val ruleList = AlarmRuleManager.getMetricRule()

    val coll = data.collect()
    println(s"addBatch(id=$batchId, dataSize=${coll.length})")
    if (coll.length > 0) {
      val df = data.sparkSession.createDataFrame(
        data.sparkSession.sparkContext.parallelize(coll), data.schema
      ).cache()

      val detectResult = ruleList.map { eventRule =>
        val metric = df.where(eventRule.metricNameFilter)
        val result = metric.withColumn("ruleId", lit(eventRule.ruleId))
          .withColumn("timestamp", current_timestamp())
          .withColumn("filter", lit(eventRule.filterStr))
          .withColumn("event", lit(eventRule.metric))
          .withColumn("value", metric("metric.value"))
          .withColumn("detect", expr(s"CASE WHEN ${eventRule.filterStr} THEN 1 ELSE 0 END"))
        //result.printSchema()
        result
      }.reduce((f1, f2) => f1.union(f2))

      if (log.isDebugEnabled) {
        println("########  detect result  ##########")
        detectResult.show(truncate = false)
      }

      detectResult
        .select(to_json(struct($"timestamp", $"ruleId", $"detect", $"resource", $"event", $"value", $"filter")).as("value"))
        .write.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("topic", topic)
        .save()
      df.unpersist()
    }
  }
}

class EventDetectSinkProvider extends StreamSinkProvider with DataSourceRegister {
  def createSink(
                  sqlContext: SQLContext,
                  parameters: Map[String, String],
                  partitionColumns: Seq[String],
                  outputMode: OutputMode): Sink = {

    new EventDetectSink(parameters)
  }

  def shortName(): String = "eventDetect"
}
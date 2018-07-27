package stresstest

import com.skt.tcore.model.Schema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.OutputMode
import stresstest.AlarmRuleRedisLoaderRDD.args


object DynamicRuleStream {

  """
     ./kafka-console-producer.sh --broker-list localhost:9092  --topic dynamic_rule
     {"id":"1", "resource":"tcore-oi-data-2-1", "metric":"mem.used_percent", "op":">", "value":50}
     {"id":"2", "resource":"tcore-oi-data-2-2", "metric":"mem.used_percent", "op":">", "value":60}
     {"id":"3", "resource":"tcore-oi-data-2-3", "metric":"mem.used_percent", "op":">", "value":70}

     {"id":"1", "resource":"tcore-oi-data-2-1", "metric":"mem.used_percent", "op":">", "value":80}
     {"id":"2", "resource":"tcore-oi-data-2-2", "metric":"mem.used_percent", "op":">", "value":90}
     {"id":"3", "resource":"tcore-oi-data-2-3", "metric":"mem.used_percent", "op":">", "value":100}

  """.stripMargin

  def main(args: Array[String]): Unit = {
    val master = if (args.length == 1) Some(args(0)) else None
    val builder = SparkSession.builder().appName("DynamicRuleStream")
    master.foreach(mst => builder.master(mst))
    implicit val spark = builder.getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    import com.skt.tcore.common.Common.{checkpointPath, kafkaServers, maxOffsetsPerTrigger, metricTopic}

    val reader = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("subscribe", "dynamic_rule")
      .option("startingOffsets", "latest") // earliest, latest

    val df = reader
      .load()
      .selectExpr("timestamp", "CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(Timestamp, String, String)]
      .toDF("timestamp", "key", "value")

    val rule = df.select($"timestamp", from_json($"value", schema = Schema.metricRuleSchema).as("data"))
      .filter($"data.id".isNotNull)
      .select("data.id", "data.resource", "data.metric", "data.value", "data.op")
      .groupBy("id").agg (
        last("resource").as("resource")
       ,last("metric").as("metric")
       ,last("value").as("value")
       ,last("op").as("op"))

    rule.printSchema()

    rule.writeStream
      .format("memory")
      .queryName("dynamic_rule")
      .outputMode(OutputMode.Complete())
      .start()

    while (true) {
      spark.sql("select * from dynamic_rule").show(truncate = false)
      Thread.sleep(5000)
    }

    spark.streams.awaitAnyTermination()
  }
}

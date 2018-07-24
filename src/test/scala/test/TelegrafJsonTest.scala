package test

import com.skt.tcore.common.Common.{checkpointPath, kafkaServers}
import com.skt.tcore.model.Schema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TelegrafJsonTest extends App {

  implicit val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("spark test")
      .config("spark.sql.streaming.checkpointLocation",  checkpointPath+"/SparkSessionTest")
      .getOrCreate()
  }


/**
  {"fields":{"active":13999108096,"available":188894736384,"available_percent":93.25854820870096,"buffered":707768320,"cached":3480580096,"free":185230909440,"inactive":1324851200,"slab":469602304,"total":202549514240,"used":13130256384,"used_percent":6.482492161616354,"wired":0},
   "name":"mem",
   "tags":{"host":"tcore-oi-data-4"},
   "timestamp":1529978760
  }
*/

  import spark.implicits._

  val metricSchema = new StructType()
    .add("name", StringType, true)
    .add("tags", MapType(StringType, StringType, true))
    .add("fields", MapType(StringType, DoubleType, true))
    .add("timestamp", TimestampType, true)

  val kafkaDF = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaServers)
    .option("subscribe", "test-metric")
    .option("startingOffsets", "latest")
    .load()
    .selectExpr("timestamp", "CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(Timestamp, String, String)].toDF("timestamp", "key", "value")

  //implicit val sc = spark.sparkContext

  val metric = kafkaDF.select($"timestamp", from_json($"value", schema = metricSchema).as("data"))
    .filter($"data.name".isNotNull)
    .withColumn("timestamp", when($"data.timestamp".isNotNull, $"data.timestamp").otherwise($"timestamp"))
    .withColumn("resource", $"data.tags.host")
    .select($"timestamp", $"resource", $"data.name", explode($"data.fields").as(Seq("metric","value")))
    .withColumn("metric", concat($"name", lit("_"), $"metric" ))

  metric.printSchema()

  metric.createOrReplaceTempView("metric")

  metric.writeStream.format("console").option("truncate",false) .start()

  spark.streams.awaitAnyTermination()
}

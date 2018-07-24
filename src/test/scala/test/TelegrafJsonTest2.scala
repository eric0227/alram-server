package test

import com.skt.tcore.common.Common.{checkpointPath, kafkaServers}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TelegrafJsonTest2 extends App {

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

  implicit val sc = spark.sparkContext

  val df = sc.parallelize(
    Seq(("""{"fields":{"active":13999108096,"available":188894736384,"available_percent":93.25854820870096,"buffered":707768320,"cached":3480580096,"free":185230909440,"inactive":1324851200,"slab":469602304,"total":202549514240,"used":13130256384,"used_percent":6.482492161616354,"wired":0},"name":"mem","tags":{"host":"tcore-oi-data-4"},"timestamp":1529978760}"""))
  ).toDF("value")


  val metric = df.select(from_json($"value", schema = metricSchema).as("data"))
    .filter($"data.name".isNotNull)
    .withColumn("resource", $"data.tags.host")
    //.select($"resource", $"data.name", $"data.fields.key".as("metric"), $"data.fields.value".as("value"))
    .select(explode($"data.fields").as(Seq("metric","value")))

  metric.printSchema()
  metric.show(truncate = false)

  spark.streams.awaitAnyTermination()

}

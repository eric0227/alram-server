package test

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.scalatest.FunSuite

import scala.concurrent.duration._


class TimestampSortingTest extends FunSuite  with SparkSessionTestWrapper with DatasetComparer {
  val bootstrap = "192.168.203.105:9092"
  val subscribe = "test111"
  import spark.implicits._

  test("watermark") {
    import spark.implicits._

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", subscribe)
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("timestamp", "CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(Timestamp, String, String)].toDF("timestamp", "key", "value")

    kafkaDF
      .writeStream
      .format("test.HBaseSinkProvider")
      .option("hbasecat", "hbase table schema")
      .option("sort", "timestamp")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime(30.seconds))
      .start()
      .awaitTermination()
  }
}

class HBaseSink(options: Map[String, String]) extends Sink with Logging {

  // String with HBaseTableCatalog.tableCatalog
  private val hBaseCatalog = options.get("hbasecat").map(_.toString).getOrElse("")
  private val sort = options.get("sort")

  override def addBatch(batchId: Long, data: DataFrame): Unit = synchronized {
    val coll = data.collect()
    println(s"addBatch(id=$batchId, dataSize=${coll.length})")

  }
}

class HBaseSinkProvider extends StreamSinkProvider with DataSourceRegister {
  def createSink(
                  sqlContext: SQLContext,
                  parameters: Map[String, String],
                  partitionColumns: Seq[String],
                  outputMode: OutputMode): Sink = {
    new HBaseSink(parameters)
  }

  def shortName(): String = "hbase"
}



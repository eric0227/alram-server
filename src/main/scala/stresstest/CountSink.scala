package stresstest

import java.util.Date

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.Sink

class CountSink(options: Map[String, String]) extends Sink with Logging {
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    println(new Date().toString + "::" + data.collect().map(_.getInt(0)).sum)
  }
}

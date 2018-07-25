package stresstest

import java.util.Date

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.Sink

class CountSink(options: Map[String, String]) extends Sink with Logging {
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    //println(new Date().toString + ", total : " + data.collect().map(_.getInt(0)).sum)
    println(new Date().toString)

    val rows = data.collect()
    rows.map { row =>
      (row.getString(0), row.getInt(1), row.getInt(2))
    }.foreach(println)

    println()
  }
}

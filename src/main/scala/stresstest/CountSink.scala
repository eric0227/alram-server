package stresstest

import java.util.Date

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.Sink

class CountSink(options: Map[String, String]) extends Sink with Logging {

  @volatile var totalMs: Long = 0
  @volatile var totalCount: Long = 0
  @volatile var start: Date = _

  println("create CountSink..")

  override def addBatch(batchId: Long, data: DataFrame): Unit = {

    if(start == null) start = new Date()
    println(s"batch_id: ${batchId}")

    val local = new Date()
    val rows = data.collect()
    val end = new Date()

    rows.map { row =>
      (row.getString(0), row.getInt(1), row.getInt(2))
    }.groupBy(_._1).map { s =>
      val check0 = s._2.filter(_._2 == 0).map(_._3).sum
      val check1 = s._2.filter(_._2 == 1).map(_._3).sum
      (s._1, check0, check1)
    }.foreach { t =>
      val count = t._2 + t._3
      totalCount = totalCount + count
      println(s"metric: ${t._1}, alarm_detect: ${t._3}, batch_count: ${t._2 + t._3}, total_count: ${totalCount}")
    }
    println(s"date: ${end.toString}, timestamp: ${start.getTime}ms, batch_time: ${end.getTime - local.getTime}ms, total_time: ${end.getTime - start.getTime}ms")
  }
}

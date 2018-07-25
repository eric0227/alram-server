package stresstest

import java.util.Date

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.Sink

class CountSink(options: Map[String, String]) extends Sink with Logging {
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    //println(new Date().toString + ", total : " + data.collect().map(_.getInt(0)).sum)
    val d = new Date()
    println(d.toString, d.getTime)

    val rows = data.collect()
    rows.map { row =>
      (row.getString(0), row.getInt(1), row.getInt(2))
    }.groupBy(_._1).map { s =>
      val check0 = s._2.filter(_._2 == 0).map(_._3).sum
      val check1 = s._2.filter(_._2 == 1).map(_._3).sum
      (s._1, check0, check1)
    }.foreach { t =>
      println(s"metric: ${t._1}, total: ${t._2 + t._3}, alarmDetect: ${t._3}")
    }
    println()
  }
}

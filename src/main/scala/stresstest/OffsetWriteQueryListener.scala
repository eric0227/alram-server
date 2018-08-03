package stresstest

import java.io.{File, FileWriter, PrintWriter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.{SourceProgress, StreamingQueryListener, StreamingQueryProgress}

class OffsetWriteQueryListener(queryName: String) extends StreamingQueryListener with Logging {


  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    println(s"start query id: ${event.id}, name: ${event.name}")
  }
  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    println(s"stop query id: ${event.id}")

  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val progress = event.progress
    println(progress.prettyJson)
  }

  def writeOffset(progress: StreamingQueryProgress, source: SourceProgress): Unit = {
  }
}

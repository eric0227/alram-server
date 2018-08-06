package stresstest

import java.io.{File, FileWriter, PrintWriter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.{SourceProgress, StreamingQueryListener, StreamingQueryProgress}

class OffsetWriteQueryListener(queryName: String) extends StreamingQueryListener with Logging {

  val outputPath = KafkaOffsetManager.getOffsetPath(queryName)
  val dir = new File(outputPath).getParentFile
  if(!dir.exists()) dir.mkdirs()
  val out = new PrintWriter(new FileWriter(outputPath, true))

  private var lastOffset = KafkaOffsetManager.getLastOffset(queryName)

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    println(s"start query id: ${event.id}, name: ${event.name}")
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    println(s"stop query id: ${event.id}")
    out.flush()
    out.close()
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val progress = event.progress
    //println(progress.prettyJson)

    if(progress.name == queryName) {
        writeOffset(progress)
    }
  }

  def writeOffset(progress: StreamingQueryProgress): Unit = {
    println(progress.durationMs)
    val date = KafkaOffsetManager.timestampFormat.parse(progress.timestamp)
    val offsets = progress.sources.map(_.endOffset).mkString(",")
    if(lastOffset != offsets) {
      val result = KafkaOffsetManager.offsetString(date, offsets)
      out.println(result)
      out.flush()
      lastOffset = offsets
    }
  }
}

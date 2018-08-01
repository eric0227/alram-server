package stresstest

import java.io.{File, FileWriter, PrintWriter}
import java.text.SimpleDateFormat

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.streaming.{SourceProgress, StreamingQueryListener, StreamingQueryProgress}

class OffsetWriteQueryListener(queryName: String, topic: String, outputPath: String) extends StreamingQueryListener {

  val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
  timestampFormat.setTimeZone(DateTimeUtils.getTimeZone("UTC"))

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  val f = new File(outputPath)
  if(!f.exists()) f.mkdirs()

  val out = new PrintWriter(new FileWriter(f))

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

    if(progress.name == queryName) {
      val sourceOpt = progress.sources.filter(_.description.contains(s"[$topic]")).headOption
      if(sourceOpt.isDefined) {
        writeOffset(progress, sourceOpt.get)
      }
    }
  }

  var startOffset: String = _
  def writeOffset(progress: StreamingQueryProgress, source: SourceProgress): Unit = {
    val date = timestampFormat.parse(progress.timestamp)

    if(startOffset != source.startOffset) {
      val result = s"${date.getTime}, ${dateFormat.format(date)}, ${source.startOffset}"
      println(result)
      out.println(result)
    }
  }
}

package stresstest

import java.io.{File, FileWriter, PrintWriter}
import java.text.SimpleDateFormat

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.streaming.{SourceProgress, StreamingQueryListener, StreamingQueryProgress}

import scala.io.Source

class OffsetWriteQueryListener(queryName: String, topic: String, outputPath: String) extends StreamingQueryListener with Logging {

  val TOKEN = "#"

  val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
  timestampFormat.setTimeZone(DateTimeUtils.getTimeZone("UTC"))
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  val dir = new File(outputPath).getParentFile
  if(!dir.exists()) dir.mkdirs()
  val out = new PrintWriter(new FileWriter(outputPath, true))

  var lastOffset = getLastOffset()

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

  def getLastOffset() = {
    val src = Source.fromFile(outputPath)
    val lastOffset = src.getLines().toList.lastOption.map { line =>
      val tokens = line.split(TOKEN)
      if (tokens.length == 3) tokens(2) else ""
    }.getOrElse("")
    src.close()
    lastOffset
  }

  def writeOffset(progress: StreamingQueryProgress, source: SourceProgress): Unit = {
    val date = timestampFormat.parse(progress.timestamp)

    if(lastOffset != source.startOffset) {
      val result = s"${date.getTime}$TOKEN${dateFormat.format(date)}$TOKEN${source.startOffset}"

      out.println(result)
      out.flush()
      lastOffset = source.startOffset
    }
  }
}

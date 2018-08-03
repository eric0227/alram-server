package stresstest

import java.text.SimpleDateFormat
import java.util.Date

import com.skt.tcore.common.Common
import org.apache.spark.sql.catalyst.util.DateTimeUtils

import scala.io.Source
import scala.util.Try

object OffsetManager {
  val TOKEN = "#"
  val offsetPath = Common.offsetPath
  val offsetFileName = "offset.out"

  val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
  timestampFormat.setTimeZone(DateTimeUtils.getTimeZone("UTC"))
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def getOffsetPath(topic: String) = offsetPath + "/" + topic + "/" + offsetFileName

  def getLastOffset(topic: String) = {
    val path = getOffsetPath(topic)
    Try {
      val src = Source.fromFile(path)
      val lastOffset = src.getLines().toList.lastOption.map { line =>
        val tokens = line.split(TOKEN)
        if (tokens.length == 3) tokens(2) else ""
      }.getOrElse("")
      src.close()
      lastOffset
    }.getOrElse("")
  }

  def offsetString(date: Date, offset: String) = {
    s"${date.getTime}$TOKEN${dateFormat.format(date)}$TOKEN${offset}"
  }

  def parseTimestamp(line: String) = line.split(TOKEN)(0).toLong

  def parseOffset(line: String) = line.split(TOKEN)(2)

  def getOffset(topic: String, date: String): Option[String] = getOffset(topic,  dateFormat.parse(date).getTime)

  def getOffset(topic: String, timestamp: Long): Option[String] = {
    val path = getOffsetPath(topic)
    Try {
      val src = Source.fromFile(path)
      val lastOffsetOpt = src.getLines().toList.sliding(2,1).find { list =>
        if(list.size == 2) {
          if (timestamp <= parseTimestamp(list(1))) true else false
        } else false
      }.map { list =>
        parseOffset(list(0))
      }
      src.close()
      lastOffsetOpt
    }.getOrElse(None)
  }

  def main(args: Array[String]): Unit = {

  }

}


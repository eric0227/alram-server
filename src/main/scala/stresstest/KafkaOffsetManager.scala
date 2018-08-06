package stresstest

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import com.skt.tcore.common.Common
import org.apache.spark.sql.catalyst.util.DateTimeUtils

import scala.io.Source
import scala.util.Try

object KafkaOffsetManager {
  val TOKEN = "#"
  val backupCheckpointPath = Common.backupCheckpointPath
  val offsetFileName = "offset.out"

  val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
  timestampFormat.setTimeZone(DateTimeUtils.getTimeZone("UTC"))
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def getCheckopintPath(queryName: String) = backupCheckpointPath + "/" + queryName

  def getOffsetPath(queryName: String) = backupCheckpointPath + "/" + queryName + "/" + offsetFileName

  def getLastOffset(queryName: String) = {
    val path = getOffsetPath(queryName)
    Try {
      val src = Source.fromFile(path)
      val lastOffset = src.getLines().toList.lastOption.map { line =>
        val tokens = line.split(TOKEN)
        if (tokens.length == 2) tokens(1) else ""
      }.getOrElse("")
      src.close()
      lastOffset
    }.getOrElse("")
  }

  def offsetString(date: Date, offset: String) = {
    s"${date.getTime}$TOKEN${offset}"
  }

  def parseTimestamp(line: String) = line.split(TOKEN)(0).toLong

  def parseOffset(line: String) = line.split(TOKEN)(1)

  def getOffset(queryName: String, date: String): Option[String] = getOffset(queryName,  dateFormat.parse(date).getTime)

  def getOffset(queryName: String, timestamp: Long): Option[String] = {
    val path = getOffsetPath(queryName)
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

  def cleanOffset(queryName: String): Unit = {

    val commitsPath = getCheckopintPath(queryName) + "/commits"
    val offsetsPath = getCheckopintPath(queryName) + "/offsets"
    val sourcesPath = getCheckopintPath(queryName) + "/sources"

    cleanDir(commitsPath)
    cleanDir(offsetsPath)
    cleanDir(sourcesPath)
  }

  def cleanDir(path: String): Unit = {
    new File(path).listFiles().foreach(_.delete())
  }

  def main(args: Array[String]): Unit = {
    println(KafkaOffsetManager.getOffsetPath("checkpoint_test"))
    println(KafkaOffsetManager.getLastOffset("checkpoint_test"))
    println(KafkaOffsetManager.getOffset("checkpoint_test",1533518919905L))
  }
}


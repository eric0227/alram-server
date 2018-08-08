package stresstest

import java.io.File
import java.nio.file.{Files, Paths, StandardOpenOption}
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

  def getCheckpointPath(queryName: String) = Common.checkpointPath + "/" + queryName

  def getBackupOffsetPath(queryName: String) = backupCheckpointPath + "/" + queryName + "/" + offsetFileName

  def getLastOffset(queryName: String) = {
    val path = getBackupOffsetPath(queryName)
    Try {
      val src = Source.fromFile(path)
      val lastOffset = src.getLines().toList.lastOption.map { line =>
        val tokens = line.split(TOKEN)
        if (tokens.length == 3) tokens(1) else ""
      }.getOrElse("")
      src.close()
      lastOffset
    }.getOrElse("")
  }

  def offsetString(date: Date, offset: String) = s"${date.getTime}$TOKEN${dateFormat.format(date)}$TOKEN${offset}"

  def parseTimestamp(line: String) = line.split(TOKEN)(0).toLong

  def parseDate(line: String) = line.split(TOKEN)(1)

  def parseOffset(line: String) = line.split(TOKEN)(2)

  def getOffset(queryName: String, date: String): Option[String] = getOffset(queryName,  dateFormat.parse(date).getTime)

  def getOffset(queryName: String, timestamp: Long): Option[String] = {
    val path = getBackupOffsetPath(queryName)
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

  def getBackupOffset(queryName: String) = {
    val path = getBackupOffsetPath(queryName)
    val src = Source.fromFile(path)
    try {src.getLines().toList} finally { src.close()}
  }

  def cleanBackupOffset(queryName: String, offset: String) = {
    val path = getBackupOffsetPath(queryName)
    val src = Source.fromFile(path)
    val list = src.getLines().toList
    src.close()
    list.zipWithIndex.find(_._1.contains(offset)).foreach {
      case (_, index) =>
        Files.write(Paths.get(path), list.slice(0, index).mkString("\n").getBytes)
    }
  }

  def deleteBackupOffsetFiles(queryName: String) = {
    val path = getBackupOffsetPath(queryName)
    new File(path).delete()
  }

  def deleteOffsetFiles(queryName: String) = {

    val commitsPath = getCheckpointPath(queryName) + "/commits"
    val offsetsPath = getCheckpointPath(queryName) + "/offsets"
    val sourcesPath = getCheckpointPath(queryName) + "/sources"

    cleanDir(commitsPath) && cleanDir(offsetsPath) && cleanDir(sourcesPath)
  }

  def cleanDir(path: String) = {
    val f = new File(path)
    if(f.exists()) new File(path).listFiles().map(_.delete()).reduce(_ && _) else true
  }

  def main(args: Array[String]): Unit = {
    println(KafkaOffsetManager.getBackupOffsetPath("checkpoint_test"))
    println(KafkaOffsetManager.getLastOffset("checkpoint_test"))
    println(KafkaOffsetManager.getOffset("checkpoint_test",1533528363990L))
  }
}


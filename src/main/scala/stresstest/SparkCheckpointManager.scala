package stresstest

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import com.skt.tcore.common.Common
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import stresstest.KafkaOffsetManager.{parseOffset, parseTimestamp}

import scala.collection.JavaConversions._
import scala.io.Source
import scala.util.Try

object SparkCheckpointManager {
  val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
  timestampFormat.setTimeZone(DateTimeUtils.getTimeZone("UTC"))
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def offsets(checkpointLocation: String): List[File] = {
    val dir = new File(checkpointLocation + "/offsets")
    dir.listFiles().toList
  }

  def getOffsetFile(checkpointLocation: String, batchTimestamp: Long): Option[File] = {
    val files = offsets(checkpointLocation)
    val fileOpt = files.sliding(2,1).find { list =>
      if(list.size == 2) {
        if (batchTimestamp <= getBatchTimestamp(list(1))) true else false
      } else false
    }.map { list =>
      list(1)
    }
    fileOpt
  }

  def getBatchTimestamp(f: File): Long = {
    val src = Source.fromFile(f)
    try {
      val line = src.getLines().toList.filter(_.contains("batchTimestampMs")).head
      val st = line.indexOf("batchTimestampMs")
      val ed = line.indexOf(",", st )
      val batchTimestampMs = line.substring(st + 16, ed).replace("\"", "").replace(":", "").toLong
      batchTimestampMs
    } finally {
      src.close()
    }
  }

  def printOffsets(checkpointLocation: String): Unit = {
    val files = offsets(checkpointLocation)
    files.foreach { f =>
      println(s"offset file: ${f.getName()}, batchTimestampMs: ${getBatchTimestamp(f)}")
    }
  }

  def printOffsets(checkpointLocation: String, batchTimestamp: Long): Unit = {
    println("rollback timestamp => "+batchTimestamp)
    println("======================================================")

    val files = offsets(checkpointLocation)
    val offsetFileOpt = getOffsetFile(checkpointLocation, batchTimestamp)
    files.foreach { f =>
      val point = offsetFileOpt.filter(_.getName() <= f.getName()).map(_ => "[x]").getOrElse("   ")
      println(s"$point offset file: ${f.getName()}, batchTimestampMs: ${getBatchTimestamp(f)}")
    }
  }

  def rollbackCheckpoint(checkpointLocation: String, batchTimestamp: Long): Boolean = {
    val files = offsets(checkpointLocation)
    val offsetFileOpt = getOffsetFile(checkpointLocation, batchTimestamp)
    Try {
      files.foreach { f =>
        offsetFileOpt.filter(_.getName() <= f.getName()).foreach { _ =>
          val cf = getCommitFile(f)
          f.delete()
          cf.delete()
          println("delete offsetFile :" + f.getPath + ", commitFile: " + cf.getPath)
        }
      }
      true
    }.recover {
      case e: Exception =>
        e.printStackTrace()
        false
    }.getOrElse(false)
  }

  def getCommitFile(offsetFile: File): File = {
    val offset = offsetFile.getName()
    val commitFile = offsetFile.getParentFile().getParentFile().getPath + "/commits/" + offset
    new File(commitFile)
  }

  def main(args: Array[String]): Unit = {
    val checkpointLocation = "d:/workspace/alarm-server/_checkpoint/checkpoint_test"

    val timestamp = "1533272734947".toLong
    SparkCheckpointManager.printOffsets(checkpointLocation, timestamp)
    rollbackCheckpoint(checkpointLocation, timestamp)
    SparkCheckpointManager.printOffsets(checkpointLocation, timestamp)
  }
}


package stresstest

import java.io.File

import com.skt.tcore.common.Common
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import stresstest.AlarmDetectionStressRddJoinTest.args

object FileSourceTest extends App {
  val master = if (args.length == 1) Some(args(0)) else None
  val builder = SparkSession.builder().appName("spark test")
  master.foreach(mst => builder.master(mst))
  implicit val spark = builder.getOrCreate()

  val ruleFilePath = Common.ruleFilePath
  val f = new File(ruleFilePath)
  if(!f.exists()) f.mkdirs()
  f.listFiles().foreach(_.delete())

  import spark.implicits._
  val df = spark.readStream
    .format("text")
    .option("maxFilesPerTrigger", 1)
    .load(ruleFilePath)
    .writeStream
    .format("memory")
    .queryName("rules")
    .outputMode(OutputMode.Update())
    .start()

  (1 to 100) foreach { i =>
    spark.sql("select * from rules").show()
    Thread.sleep(3000)
  }

  spark.streams.awaitAnyTermination()

}

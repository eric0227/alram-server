package sink

import java.sql.Timestamp

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.skt.tcore.AlarmRuleManager
import com.skt.tcore.model.{Alarm, ContinuousAlarmRuleAccumulator}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode

class ContinuousAlarmDetectSink(options: Map[String, String]) extends Sink with Logging {
  @volatile private var latestBatchId = -1L

  val bootstrap = options.get("kafka.bootstrap.servers").get
  val topic = options.get("topic").get

  // debug
  System.out.println(System.getenv())

  // test
  AlarmRuleManager.createDummyRule()

  val ruleList = AlarmRuleManager.getContinuousAlarmRule()
  val ruleAccumList = ruleList.map(r => ContinuousAlarmRuleAccumulator(r))

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    import data.sparkSession.implicits._

    val coll = data.collect()
    println(s"addBatch(id=$batchId, dataSize=${coll.length})")

    if (coll.length > 0) {
      val detectedList = coll.flatMap { row =>
        val ruleId = row.getAs[String]("ruleId")
        val detect = row.getAs[Boolean]("detect")
        val occurTimestamp = row.getAs[Timestamp]("timestamp")

        ruleAccumList.filter { accum =>
          if(ruleId == accum.continueRule.targetRuleId) {
            if (detect) accum.increase() else accum.reset()
            accum.occurTimestamp = occurTimestamp
            accum.check()
          } else false
        }
      }

      val alarm = detectedList.map { d =>
        val payload = mapper.writeValueAsString(Map("rule" -> d.continueRule))
        Alarm(alarmType="continuous", ruleId=d.continueRule.ruleId, detect=true, occurCount=d.occurCount, occurTimestamp=d.occurTimestamp, payload=payload)
      }

      if(!alarm.isEmpty) {
        val df = data.sparkSession.createDataFrame(
          data.sparkSession.sparkContext.parallelize(alarm)
        )

        if (log.isWarnEnabled) {
          println("########  alarm  ##########")
          df.show(truncate = false)
        }

        df.select(to_json(struct("alarmType", "ruleId", "detect", "occurCount", "occurTimestamp", "payload")).as("value"))
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", bootstrap)
          .option("topic", topic)
          .save()
      }
    }
  }

}

class ContinuousAlarmDetectSinkProvider extends StreamSinkProvider with DataSourceRegister {
  def createSink(
                  sqlContext: SQLContext,
                  parameters: Map[String, String],
                  partitionColumns: Seq[String],
                  outputMode: OutputMode): Sink = {
    new ContinuousAlarmDetectSink(parameters)
  }

  def shortName(): String = "ContinuousAlarmDetect"
}

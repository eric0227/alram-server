package test

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.{Column, Row, SQLContext}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.scalatest.FunSuite

import scala.reflect.internal.util.TableDef.Column

class UDAFTest extends FunSuite with SparkSessionTestWrapper with DatasetComparer  with Serializable  {

  class Avg extends UserDefinedAggregateFunction with Serializable {
    override def inputSchema: StructType =
      new StructType().add("value", DoubleType)

    override def bufferSchema: StructType =
      new StructType()
        .add("count", LongType)
        .add("sum", DoubleType)

    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0.0
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getAs[Long](0) + 1
      buffer(1) = buffer.getAs[Double](1) + input.getAs[Double](0)
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
      buffer1(1) = buffer1.getAs[Double](1) + buffer2.getAs[Double](1)
    }

    override def evaluate(buffer: Row): Any =
      buffer.getDouble(1) / buffer.getLong(0)
  }

  test("Avg UDAF") {
      val sqlCtx: SQLContext = spark.sqlContext
      val avg = new Avg
      sqlCtx.udf.register("userAvg", avg)

      import spark.implicits._
      import org.apache.spark.sql.functions._

      val data = Seq((12), (11), (1), (0))
      val df = data.toDF("value")
      df.createOrReplaceTempView("test")
      df.show()
      df.agg(count("value")).show()
      df.agg(avg($"value")).show()
      spark.sql("select userAvg(value) from test").show()
    }
}

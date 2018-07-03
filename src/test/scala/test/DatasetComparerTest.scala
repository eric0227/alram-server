package test

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class DatasetComparerTest extends FunSuite  with SparkSessionTestWrapper with DatasetComparer {
  import spark.implicits._

  test("DatasetComparer") {
    val sourceDF = Seq(
      ("jose"),
      ("li"),
      ("luisa")
    ).toDF("name")

    val actualDF = sourceDF.select(col("name").alias("student"))

    val expectedDF = Seq(
      ("jose"),
      ("li"),
      ("luisa")
    ).toDF("student")

    assertSmallDatasetEquality(actualDF, expectedDF)
  }
}

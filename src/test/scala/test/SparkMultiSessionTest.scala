package test

import org.apache.spark.sql.SparkSession

/**
  * Created by kyu hyoung Lee on 2018-08-09.
  */
object SparkMultiSessionTest extends App {

  case class Data(no: Int, value: Int)

  val spark = SparkSession.builder().master("local[*]").appName("SparkMultiSessionTest").getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  val ds = (1 to 100).map( n => (n % 10, n)).groupBy(_._1).map(t => Data(t._1, t._2.map(_._2).sum)).toSeq.toDS()
  ds.createOrReplaceTempView("local_view")
  ds.createOrReplaceGlobalTempView("global_view")
  ds.printSchema()
  ds.show()

  val spark2 = spark.newSession()
  spark2.sql("select * from global_temp.global_view").show()

}

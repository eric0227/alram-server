package stresstest

import org.apache.spark.sql.SparkSession

/**
  * Created by kyu hyoung Lee on 2018-08-14.
  */
object AccumulatorTest extends App {

  case class Data(no: Int, value: Int)

  //def main(args: Array[String]) {

    val master = if (args.length == 1) Some(args(0)) else None
    val builder = SparkSession.builder().appName("AccumulatorTest")
    master.foreach(mst => builder.master(mst))
    implicit val spark = builder.getOrCreate()
    import spark.implicits._

    val ds = (1 to 100).map(n => (n % 10, n)).map(t => Data(t._1, t._2)).toSeq.toDS()
    val ds2 = ds.repartition(ds("no"))
    ds2.printSchema()
    ds2.show()

    val list = spark.sparkContext.collectionAccumulator[String]
    ds2.mapPartitions { iter =>
      val l = iter.toList
      if (!l.isEmpty) {
        list.add(l.size + "")
        println(list.value)
        println(s"iter => ${l.size}, list => ${list.value.size()}")
        list.value.toArray().foreach(println)
      }
      l.iterator
    }.show()

    println("===========")
    list.value.toArray().foreach(println)
  //}
}

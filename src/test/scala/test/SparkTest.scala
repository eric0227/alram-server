package test

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.scalatest.FunSuite

class SparkTest extends FunSuite with SparkSessionTestWrapper with DatasetComparer  with Serializable  {

  case class PandaKey(city: String, zip: String, name: String)
  test("order") {
    implicit def orderBy[T <: PandaKey]: Ordering[T] = {
      Ordering.by(p => (p.city, p.zip, p.name))
    }
    //implicit val ordering = Ordering.Tuple2
  }
}

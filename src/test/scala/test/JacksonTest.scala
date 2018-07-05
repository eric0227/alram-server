package test

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.scalatest.FunSuite

class JacksonTest  extends FunSuite {

  case class UserData(name: String, age: Int)

  test("class to json") {

    val user = UserData("aaa", 28)

    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    assert("""{"name":"aaa","age":28}""" == mapper.writeValueAsString(user))
  }

}

import sbt.ExclusionRule

version := "0.1"
name := "alarm-server"
scalaVersion := "2.11.8"

val sparkVersion = "2.3.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion excludeAll ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion

libraryDependencies += "com.typesafe" % "config" % "1.3.3"
libraryDependencies += "org.scala-lang.modules" %% "scala-async" % "0.9.7"
//libraryDependencies += "net.debasishg" %% "redisclient" % "3.7"
//libraryDependencies += "com.github.etaty" %% "rediscala" % "1.8.0"
libraryDependencies += "redis.clients" % "jedis" % "2.9.0"
libraryDependencies += "io.lettuce" % "lettuce-core" % "5.0.4.RELEASE"


libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "0.11.0" % "test" excludeAll ExclusionRule(organization = "org.apache.spark")

//fork in run := true
javaOptions in run ++= Seq(
  "-Dlog4j.debug=true",
  "-Dlog4j.configuration=log4j.properties")
outputStrategy := Some(StdoutOutput)

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", xs@_*) => MergeStrategy.last
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/MANIFEST.MF" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

packageBin in Compile := file(s"${name.value}_${scalaBinaryVersion.value}.jar")

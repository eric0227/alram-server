import sbt.ExclusionRule

version := "0.1"
name := "alarm-server"
scalaVersion := "2.11.8"

val sparkVersion = "2.3.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion excludeAll ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")

libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion

libraryDependencies += "com.typesafe" % "config" % "1.3.3"
libraryDependencies += "org.scala-lang.modules" %% "scala-async" % "0.9.7"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "0.11.0" % "test" excludeAll ExclusionRule(organization = "org.apache.spark")

//libraryDependencies += "com.hortonworks" % "shc-core" % "1.1.1-2.1-s_2.11"


//fork in run := true
javaOptions in run ++= Seq(
  "-Dlog4j.debug=true",
  "-Dlog4j.configuration=log4j.properties")
outputStrategy := Some(StdoutOutput)

assemblyMergeStrategy in assembly := {
  case x if x.startsWith("META-INF/ECLIPSEF.RSA") => MergeStrategy.last
  case x if x.startsWith("META-INF/mailcap") => MergeStrategy.last
  case x if x.startsWith("plugin.properties") => MergeStrategy.last
  case x => MergeStrategy.last
}

packageBin in Compile := file(s"${name.value}_${scalaBinaryVersion.value}.jar")

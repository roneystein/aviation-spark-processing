import sun.security.tools.PathList

name := "sparkccc2"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.0" % "provided",
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.0" % "provided",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.0",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0-RC1"
)


val meta = """META.INF(.)*""".r

assemblyMergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first
}
}
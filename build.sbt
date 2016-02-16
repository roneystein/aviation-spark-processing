import sun.security.tools.PathList

name := "sparkccc2"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.0" % "provided",
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.0" % "provided",
  "joda-time" % "joda-time" % "2.9.2" % "provided",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.0",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0-RC1" excludeAll(
    ExclusionRule(organization = "org.apache.hadoop"),
    ExclusionRule(organization = "org.scala-lang"),
    ExclusionRule(organization = "org.apache.commons"),
    ExclusionRule(organization = "com.typesafe.akka"),
    ExclusionRule(organization = "commons-beanutils"),
    ExclusionRule(organization = "commons-cli"),
    ExclusionRule(organization = "commons-codec"),
    ExclusionRule(organization = "commons-collections"),
    ExclusionRule(organization = "commons-configuration"),
    ExclusionRule(organization = "commons-digester"),
    ExclusionRule(organization = "commons-httpclient"),
    ExclusionRule(organization = "commons-io"),
    ExclusionRule(organization = "commons-lang"),
    ExclusionRule(organization = "commons-net"),
    ExclusionRule(organization = "javax.inject"),
    ExclusionRule(organization = "javax.xml.bind"),
    ExclusionRule(organization = "joda-time"),
    ExclusionRule(organization = "log4j"),
    ExclusionRule(organization = "org.apache.curator"),
    ExclusionRule(organization = "org.apache.mesos"),
    ExclusionRule(organization = "org.apache.parquet")
    ))


val meta = """META.INF(.)*""".r

assemblyMergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first
}
}
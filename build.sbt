import sbt._
import Keys._
import sbtassembly.MergeStrategy

name := "spark-disjoint-ranges-task"

version := "0.1"

scalaVersion := "2.12.11"
libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % "3.0.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.0.1" % "provided",
  "org.postgresql" % "postgresql" % "42.2.18"
)

mainClass in assembly := Some("intervals.IntervalsMain")
assemblyMergeStrategy in assembly := {
  case x if x.contains("module-info.class") => MergeStrategy.discard
  case x if x.contains("git.properties") => MergeStrategy.discard
  case x if x.contains("jakarta") => MergeStrategy.first
  case x if x.contains("javax") => MergeStrategy.first
  case x if x.contains("aopalliance") => MergeStrategy.first
  case x if x.contains("apache/commons/collections") => MergeStrategy.first
  case x if x.contains("apache/spark/unused") => MergeStrategy.first
  case manifest if manifest.contains("MANIFEST.MF") =>
    MergeStrategy.discard
  case referenceOverrides if referenceOverrides.contains("reference-overrides.conf") =>
    MergeStrategy.concat
  case PathList("javax", "activation", ps @ _*) => MergeStrategy.concat
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
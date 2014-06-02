import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

name := "example-algorithm"

organization := "com.datamininghub"

version := "1.0"

scalaVersion := "2.11.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.0.3"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.2.0"

libraryDependencies += "net.sf.opencsv" % "opencsv" % "2.3"

assemblySettings

mainClass in (Compile, run) := Some("Generator.Driver")

mainClass in assembly := Some("Algorithm.Driver")

jarName in assembly := "example-algorithm.jar"

mergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

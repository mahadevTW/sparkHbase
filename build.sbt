name := "SampleDemo"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.logging.log4j" % "log4j-api" % "2.11.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.11.1",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.apache.tephra" % "tephra-core" % "0.15.0-incubating",
  "org.apache.tephra" % "tephra-api" % "0.15.0-incubating",
  "org.apache.hbase" % "hbase-client" % "1.2.0",
  "org.apache.hbase" % "hbase-common" % "1.2.0",
  "org.apache.tephra" % "tephra-hbase-compat-1.2" % "0.15.0-incubating"
)

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.6.0"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.2.6.1"

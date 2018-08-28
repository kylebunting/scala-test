name := "com.ahcusa"

version := "0.1"

scalaVersion := "2.11.8"

organization := "com.ahcusa"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-sql_2.11" % "2.0.0" % "provided",
  "com.databricks" % "dbutils-api_2.11" % "0.0.3" % "provided",
  "com.databricks" % "spark-avro_2.11" % "4.0.0" % "provided")
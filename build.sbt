name := "training-spark-structured-streaming"
scalaVersion := "2.11.8"
organization := "ilittleangel"
version in ThisBuild := "1.1.0-SNAPSHOT"

lazy val scalaMajor = "2.12"
lazy val scalaMinor = "7"
lazy val sparkVersion = "2.2.0"
lazy val scalaTestVersion = "3.0.4"
lazy val kafkaClientVersion = "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % Provided
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.10.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % kafkaClientVersion
libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion % Test
libraryDependencies += "io.github.embeddedkafka" %% "embedded-kafka" % kafkaClientVersion % Test

parallelExecution in Test := false

name := "training-spark-structured-streaming"
scalaVersion := "2.11.8"
organization := "ilittleangel"
version in ThisBuild := "1.1.0-SNAPSHOT"

lazy val sparkVersion = "2.2.0"
lazy val scalaTestVersion = "3.0.4"
lazy val holdenkarauVersion = s"${sparkVersion}_0.8.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % holdenkarauVersion % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion % "test"

parallelExecution in Test := false

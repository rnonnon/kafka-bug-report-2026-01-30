ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-bug-report",
    libraryDependencies += "org.apache.kafka" % "kafka-clients" % "4.1.1",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.18" % Test,
    libraryDependencies += "org.slf4j" % "slf4j-simple" % "2.0.12" % Test
  )

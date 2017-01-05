import sbt._

lazy val commonSettings = Seq(
    organization := "com.nexusguard",
    version := "0.1.0",
    scalaVersion := "2.12.1"
)

lazy val root = (project in file("."))
    .settings(commonSettings: _*)
    .settings(
        name := "zkpipe",
        libraryDependencies ++= Seq(
            "org.scala-lang.modules" %% "scala-async" % "0.9.6",
            "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.7",
            "org.apache.logging.log4j" % "log4j-api" % "2.7",
            "org.apache.logging.log4j" % "log4j-core" % "2.7",
            "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.5",
            "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.8.5",
            "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
            "com.google.guava" % "guava" % "20.0",
            "com.github.scopt" %% "scopt" % "3.5.0",
            "com.twitter" %% "finagle-http" % "6.41.0" exclude("com.google.guava", "guava"),
            "org.apache.kafka" % "kafka-clients" % "0.10.1.1",
            "org.apache.zookeeper" % "zookeeper" % "3.4.9" exclude("org.slf4j", "slf4j-log4j12"),
            "com.netaporter" %% "scala-uri" % "0.4.16"
        ),
        transitiveClassifiers ++= Seq("sources")
    )
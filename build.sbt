import sbt._

lazy val commonSettings = Seq(
    name := "zkpipe",
    organization := "com.nexusguard",
    version := "0.1.0",
    scalaVersion := "2.12.1",
    javacOptions ++= Seq("-source", "1.7", "-target", "1.7")
)

lazy val librarySettings = Seq(
    libraryDependencies ++= Seq(
        // logging
        "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.7",
        "org.apache.logging.log4j" % "log4j-api" % "2.7",
        "org.apache.logging.log4j" % "log4j-core" % "2.7",
        "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.5",
        "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.8.5",
        "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
        // common
        "com.google.guava" % "guava" % "20.0",
        // command line options
        "com.github.scopt" %% "scopt" % "3.5.0",
        // kafka client
        "org.apache.kafka" % "kafka-clients" % "0.10.1.1",
        // zookeeper
        "org.apache.zookeeper" % "zookeeper" % "3.5.2-alpha" exclude("org.slf4j", "slf4j-log4j12"),
        // URI
        "com.netaporter" %% "scala-uri" % "0.4.16",
        // datetime
        "joda-time" % "joda-time" % "2.9.7",
        "com.github.nscala-time" %% "nscala-time" % "2.16.0",
        // JSON encoding
        "org.json4s" %% "json4s-native" % "3.5.0",
        // protobuf encoding
        "com.google.protobuf" % "protobuf-java" % "3.1.0",
        // For finding google/protobuf/descriptor.proto
        "com.trueaccord.scalapb" %% "scalapb-runtime" % com.trueaccord.scalapb.compiler.Version.scalapbVersion % "protobuf",
        // For JSON binding
        "com.trueaccord.scalapb" %% "scalapb-json4s" % "0.1.5",
        // Prometheus metrics
        "io.prometheus" % "simpleclient" % "0.0.19",
        "io.prometheus" % "simpleclient_hotspot" % "0.0.19"
    ),
    transitiveClassifiers ++= Seq("sources")
)

lazy val assemblySettings = Seq(
    mainClass in (Compile, assembly) := Some("zkpipe.LogPipe")
)

lazy val root = (project in file("."))
    .settings(commonSettings: _*)
    .settings(librarySettings: _*)
    .settings(assemblySettings: _*)

PB.targets in Compile := Seq(
    PB.gens.java -> (sourceManaged in Compile).value,
    scalapb.gen(javaConversions = true) -> (sourceManaged in Compile).value
)

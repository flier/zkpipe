import sbt._

lazy val commonSettings = Seq(
    name := "zkpipe",
    organization := "com.nexusguard",
    version := "0.1.0",
    javacOptions ++= Seq("-source", "1.7", "-target", "1.7"),
    scalaVersion := "2.12.1"
)

lazy val librarySettings = Seq(
    libraryDependencies ++= Seq(
        // XML
        "org.scala-lang.modules" %% "scala-xml" % "1.0.6",
        // logging
        "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.7",
        "org.apache.logging.log4j" % "log4j-api" % "2.7",
        "org.apache.logging.log4j" % "log4j-core" % "2.7",
        "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.5",
        "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.8.5",
        "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
        // scala async
        "org.scala-lang.modules" %% "scala-async" % "0.9.6",
        // scalaz extentions
        "org.scalaz" %% "scalaz-core" % "7.2.8",
        "org.scalaz" %% "scalaz-concurrent" % "7.2.8",
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
        "io.prometheus" % "simpleclient_hotspot" % "0.0.19",
        "io.prometheus" % "simpleclient_servlet" % "0.0.19",
        "io.prometheus" % "simpleclient_pushgateway" % "0.0.19",
        "io.prometheus" % "simpleclient_dropwizard" % "0.0.19",
        "org.eclipse.jetty" % "jetty-servlet" % "9.4.0.v20161208",
        // DropWizard metrics with metrics-scala
        "nl.grons" %% "metrics-scala" % "3.5.5",
        "io.dropwizard.metrics" % "metrics-graphite" % "3.1.2",
        "io.dropwizard.metrics" % "metrics-ganglia" % "3.1.2",
        "io.dropwizard.metrics" % "metrics-jetty9" % "3.1.2",
        "io.dropwizard.metrics" % "metrics-servlets" % "3.1.2",
        // spec2
        "org.specs2" %% "specs2-core" % "3.8.6" % "test",
        "org.specs2" %% "specs2-scalacheck" % "3.8.6" % "test",
        "org.specs2" %% "specs2-mock" % "3.8.6" % "test"
    ),
    transitiveClassifiers ++= Seq("sources")
)

lazy val assemblySettings = Seq(
    mainClass in (Compile, assembly) := Some("zkpipe.LogPipe")
)

lazy val root = (project in file("."))
    .enablePlugins(BuildInfoPlugin)
    .settings(
        buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
        buildInfoPackage := "zkpipe"
    )
    .settings(commonSettings: _*)
    .settings(librarySettings: _*)
    .settings(assemblySettings: _*)

PB.targets in Compile := Seq(
    PB.gens.java -> (sourceManaged in Compile).value,
    scalapb.gen(javaConversions = true) -> (sourceManaged in Compile).value
)

fork in (Test, run) := true

scalacOptions in Test ++= Seq("-Yrangepos")
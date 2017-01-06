addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.3" exclude("com.google.protobuf", "protobuf-java"))

libraryDependencies += "com.trueaccord.scalapb" %% "compilerplugin" % "0.5.46"

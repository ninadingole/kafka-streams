import sbt.util

name := "kafka-streams"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "confulent" at "https://packages.confluent.io/maven/"

sourceGenerators in Compile += (avroScalaGenerate in Compile).taskValue


libraryDependencies += "io.confluent" % "kafka-streams-avro-serde" % "4.1.0"
libraryDependencies += "io.confluent" % "kafka-avro-serializer" % "4.1.0"
libraryDependencies += "io.confluent" % "kafka-schema-registry-client" % "4.1.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.1.0-cp1"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "1.1.0-cp1"
libraryDependencies += "org.apache.kafka" %% "kafka" % "1.1.0"
libraryDependencies += "org.apache.avro" % "avro" % "1.8.1"
libraryDependencies += "com.lightbend" %% "kafka-streams-scala" % "0.2.1"
libraryDependencies += "com.julianpeeters" %% "avrohugger-core" % "1.0.0-RC4"

unmanagedClasspath in Compile += baseDirectory.value / "src/main/avro"

enablePlugins(SbtAvrohugger)
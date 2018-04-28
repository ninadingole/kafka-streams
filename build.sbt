import sbt.util

name := "kafka-streams"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "confulent" at "https://packages.confluent.io/maven/"



libraryDependencies += "io.confluent" % "kafka-streams-avro-serde" % "4.1.0"
libraryDependencies += "io.confluent" % "kafka-avro-serializer" % "4.1.0"
libraryDependencies += "io.confluent" % "kafka-schema-registry-client" % "4.1.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.1.0-cp1"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "1.1.0-cp1"
libraryDependencies += "org.apache.kafka" %% "kafka" % "1.1.0"
libraryDependencies += "org.apache.avro" % "avro" % "1.8.1"
libraryDependencies += "com.lightbend" %% "kafka-streams-scala" % "0.2.1"
libraryDependencies += "com.sksamuel.avro4s" %% "avro4s-core" % "1.8.3"


avroSpecificSourceDirectory := new File("src/main/scala/avro")

sourceGenerators in Compile += (avroScalaGenerate in Compile).taskValue
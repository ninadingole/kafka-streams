addSbtPlugin("com.julianpeeters" % "sbt-avrohugger" % "2.0.0-RC4")
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)


libraryDependencies += "com.julianpeeters" % "avro-scala-macro-annotations_2.11" % "0.11.1"

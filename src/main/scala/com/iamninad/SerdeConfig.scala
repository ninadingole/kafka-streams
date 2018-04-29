package com.iamninad

import scala.collection.JavaConverters._

class SerdeConfig {

  def get(): java.util.Map[String, String] = {
    Map("schema.registry.url" -> "http://localhost:8081/",
      "specific.avro.reader" -> "false").asJava
  }

}

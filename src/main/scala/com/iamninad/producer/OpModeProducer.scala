package com.iamninad.producer

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer

class OpModeProducer(val config: Properties) {

  val producer: KafkaProducer[String, Array[Byte]] = new KafkaProducer[String, Array[Byte]](config)



}

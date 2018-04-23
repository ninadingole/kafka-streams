package com.iamninad

import java.nio.ByteBuffer
import java.util.Properties

import com.iamninad.model.{Employee, GenericWrapper}
import com.iamninad.producer.TxModeProducer
import com.iamninad.serializer.EmployeeSerializer
import com.lightbend.kafka.scala.streams.StreamsBuilderS
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.kafka.common.utils.ByteBufferOutputStream
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.KStreamBuilder

object HelloWorld extends App {

  val config = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "helloWorld")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props
  }

  val employee = new Employee("Swapnil", 22)
  val employee1 = new Employee("Sagar", 27)
  println(this.getClass.getClassLoader.getResource(".").getFile)

  private val bytes: Array[Byte] = new EmployeeSerializer().serialize(employee)
  private val bytes1: Array[Byte] = new EmployeeSerializer().serialize(employee1)

  val data = Array(GenericWrapper("employee", 12323L,bytes),
    GenericWrapper("employee", 23423L,bytes1))

  new TxModeProducer(config).sendMessage(data, "^", "employee")

  val builder = new StreamsBuilderS()

  private val streams = new KafkaStreams(builder.build(), config)
  //  streams.start()

}

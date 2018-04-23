package com.iamninad.producer

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.Properties

import com.iamninad.model.GenericWrapper
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

class TxModeProducer(val config:Properties) {

  val schemaSource = Source.fromURI(this.getClass.getClassLoader.getResource("avro/generic_wrapper.avsc").toURI)
  val schema = new Schema.Parser().parse(schemaSource.mkString)
  var producer: KafkaProducer[String, Array[Byte]] = null

  def sendMessage(payloads: Array[GenericWrapper], delimiter: String, topic: String) = {
    openProducer()
    val buffer = new ByteArrayOutputStream()

    val writer = new SpecificDatumWriter[GenericRecord](schema)
    val encoder = EncoderFactory.get().binaryEncoder(buffer, null)

    payloads.foreach( wrapper => {
      val record = new GenericData.Record(schema)
      record.put("table_name", wrapper.tableName)
      record.put("schema_fingerprint", wrapper.schema_fingerprint)
      record.put("payload", ByteBuffer.wrap(wrapper.payload))

      writer.write(record, encoder)
      encoder.flush()
      buffer.write(delimiter.getBytes)
    })

    buffer.close()

    producer.send(new ProducerRecord[String, Array[Byte]]( topic, System.currentTimeMillis().toString, buffer.toByteArray))
    close()
  }

  private def openProducer(): Unit = {
    producer = new KafkaProducer[String, Array[Byte]](config)
  }

  private def close(): Unit = {
    producer.close()
  }
}

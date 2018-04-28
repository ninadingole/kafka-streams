package com.iamninad.deserializer

import com.iamninad.HelloWorld.serdeConfig
import com.lightbend.kafka.scala.streams.Deserializer
import com.sksamuel.avro4s.RecordFormat
import dbserver1.moviedemo.MOVIE.Envelope
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord

class AbstractDeserializer[T >: Null] extends Deserializer[T]{
  override def deserialize(data: Array[Byte]): Option[T] = {
    val deserializer = new KafkaAvroDeserializer()
    deserializer.configure(serdeConfig, false)
    val record = deserializer.deserialize(null, data).asInstanceOf[GenericRecord]
    val recordFormat = RecordFormat[Envelope]
    val envelope:Envelope = recordFormat.from(record)
    val value = envelope.after.get

    if(value == null) Option.empty[T] else Option(value.asInstanceOf[T])
  }
}

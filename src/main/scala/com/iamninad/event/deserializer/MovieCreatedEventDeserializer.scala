package com.iamninad.event.deserializer

import com.iamninad.AppSerdes
import com.iamninad.model.BusinessEvent
import com.iamninad.util.Utils
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.mongodb.scala.Document

import scala.collection.JavaConverters._

class MovieCreatedEventDeserializer(event: BusinessEvent) {

  private val schemaConfig = Map("schema.registry.url" -> "http://localhost:8081", "auto.register.schemas" -> "true").asJava

  def get: Option[Document] = {
    var doc: Option[Document] = null
    if (!event.events.isEmpty) {

      val deserializer = new KafkaAvroDeserializer()
      deserializer.configure(schemaConfig, false)
      val events               = event.events
      val maybeBytes           = events.get("movie")
      val value: GenericRecord = deserializer.deserialize(Utils.getTopic("movie"), maybeBytes.get).asInstanceOf[GenericRecord]
      val movie                = AppSerdes.movieBEventSerde.movieFormat.from(value)

      val saleByteMessagae  = events.get("sale")
      val genericRecordSale = deserializer.deserialize(Utils.getTopic("sale"), saleByteMessagae.get).asInstanceOf[GenericRecord]
      val sales             = AppSerdes.movieBEventSerde.saleFormat.from(genericRecordSale)

      if (movie != null && sales != null) {
        doc = Option(
          Document("movie_id" -> movie.movie_id,
                   "title"    -> movie.title,
                   "year"     -> movie.year,
                   "budget"   -> movie.budget,
                   "sales"    -> Document("total" -> sales.total))
        )
      }
    } else {
      doc = Option.empty[Document]
    }
    doc
  }

}

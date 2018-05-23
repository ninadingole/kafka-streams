package com.iamninad.event.deserializer

import com.iamninad.AppSerdes
import com.iamninad.model.BusinessEvent
import com.iamninad.util.Utils
import dbserver1.moviedemo.movie.{Envelope, Movie}
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.mongodb.scala.Document
import scala.collection.JavaConverters._

class MovieUpdateEventDeserializer(event: BusinessEvent) {
  private val schemaConfig = Map("schema.registry.url" -> "http://localhost:8081", "auto.register.schemas" -> "true").asJava

  def get: Option[Document] = {
    var doc: Option[Document] = null
    if (!event.events.isEmpty) {
      val deserializer = new KafkaAvroDeserializer()
      deserializer.configure(schemaConfig, false)
      val beforeMovieBytes   = event.events("before")
      val genericRecord      = deserializer.deserialize(Utils.getTopic("movie"), beforeMovieBytes).asInstanceOf[GenericRecord]
      val beforeMovie: Movie = AppSerdes.movieBEventSerde.movieFormat.from(genericRecord)

      val afterMovieBytes   = event.events("after")
      val genericRecord1    = deserializer.deserialize(Utils.getTopic("movie"), afterMovieBytes).asInstanceOf[GenericRecord]
      val afterMovie: Movie = AppSerdes.movieBEventSerde.movieFormat.from(genericRecord1)
      if (afterMovie != null) {
        doc = Option(
          Document("movie_id" -> beforeMovie.movie_id,
                   "title"    -> afterMovie.title,
                   "year"     -> afterMovie.year,
                   "budget"   -> afterMovie.budget)
        )

      } else {
        doc = Option.empty[Document]
      }
    }
    doc
  }
}

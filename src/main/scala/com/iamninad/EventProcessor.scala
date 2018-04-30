package com.iamninad

import java.util.Properties

import com.iamninad.model.BusinessEvent
import com.lightbend.kafka.scala.streams.StreamsBuilderS
import io.confluent.kafka.serializers.{
  AbstractKafkaAvroSerDeConfig,
  KafkaAvroDeserializer,
  KafkaAvroDeserializerConfig,
  KafkaAvroSerializer
}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase}

import scala.collection.JavaConverters._

object EventProcessor extends App {

  import scala.concurrent.ExecutionContext.Implicits.global

  val config = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-6")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", classOf[KafkaAvroSerializer].getName)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.deserializer", classOf[KafkaAvroDeserializer].getName)
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true")
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081/")
    props
  }

  private val builder = new StreamsBuilderS()

  private val schemaConfig = Map("schema.registry.url" -> "http://localhost:8081", "auto.register.schemas" -> "true").asJava

  def buildEventStream = {
    import AppSerdes.movieBEventSerde.eventConsumed
    builder.stream[Int, BusinessEvent]("events")
  }

  buildEventStream.foreach((id, event) => {
    val mongoClient                       = MongoClient("mongodb://localhost:27017")
    val moviedemoDatabase: MongoDatabase  = mongoClient.getDatabase("moviedemo")
    val movies: MongoCollection[Document] = moviedemoDatabase.getCollection("movies")

    if (!event.events.isEmpty) {

      val deserializer = new KafkaAvroDeserializer()
      deserializer.configure(schemaConfig, false)
      val events               = event.events
      val maybeBytes           = events.get("movie")
      val value: GenericRecord = deserializer.deserialize("events", maybeBytes.get).asInstanceOf[GenericRecord]
      val movie                = AppSerdes.movieBEventSerde.movieFormat.from(value)
      println(movie)

      val saleByteMessagae  = events.get("sale")
      val genericRecordSale = deserializer.deserialize("events", saleByteMessagae.get).asInstanceOf[GenericRecord]
      val sales             = AppSerdes.movieBEventSerde.saleFormat.from(genericRecordSale)
      println(sales)

      val doc = Document("movie_id" -> movie.movie_id,
                         "title"  -> movie.title,
                         "year"   -> movie.year,
                         "budget" -> movie.budget,
                         "sales"  -> Document("total" -> sales.total))

      movies.insertOne(document = doc).toFuture().onComplete(_ => println(s"Inserted ${doc}"))

    }

  })

  private val streams = new KafkaStreams(builder.build(), config)
  streams.start()

}

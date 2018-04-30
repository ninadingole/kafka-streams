package com.iamninad

import java.util.Properties

import com.iamninad.event.deserializer.MovieCreatedEventDeserializer
import com.iamninad.model.BusinessEvent
import com.lightbend.kafka.scala.streams.StreamsBuilderS
import io.confluent.kafka.serializers.{
  AbstractKafkaAvroSerDeConfig,
  KafkaAvroDeserializer,
  KafkaAvroDeserializerConfig,
  KafkaAvroSerializer
}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase}

import scala.collection.JavaConverters._

object EventProcessor extends App {

  import scala.concurrent.ExecutionContext.Implicits.global

  val config = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-10")
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

    val value = new MovieCreatedEventDeserializer(event).get
    if (value.isDefined) {
      val doc = value.get
      movies.insertOne(document = doc).toFuture().onComplete(_ => println(s"Inserted ${doc}"))
    }

  })

  private val streams = new KafkaStreams(builder.build(), config)
  streams.start()

}

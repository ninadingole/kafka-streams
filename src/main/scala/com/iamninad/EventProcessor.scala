package com.iamninad

import java.util.Properties

import com.iamninad.event.deserializer.MovieCreatedEventDeserializer
import com.iamninad.model.BusinessEvent
import com.iamninad.util.EventTypes
import com.lightbend.kafka.scala.streams.{KStreamS, StreamsBuilderS}
import io.confluent.kafka.serializers.{
  AbstractKafkaAvroSerDeConfig,
  KafkaAvroDeserializer,
  KafkaAvroDeserializerConfig,
  KafkaAvroSerializer
}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase}

object EventProcessor extends App {

  import scala.concurrent.ExecutionContext.Implicits.global

  val config = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-11")
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

  def buildEventStream = {
    import AppSerdes.movieBEventSerde.eventConsumed
    builder.stream[Int, BusinessEvent]("events")
  }

  val mongoClient                       = MongoClient("mongodb://localhost:27017")
  val moviedemoDatabase: MongoDatabase  = mongoClient.getDatabase("moviedemo")
  val movies: MongoCollection[Document] = moviedemoDatabase.getCollection("movies")

  private val eventStreams: KStreamS[Int, BusinessEvent] = buildEventStream

  def filterEventsByType(eventType: String): KStreamS[Int, BusinessEvent] = {
    eventStreams.filter((_: Int, event: BusinessEvent) => event.eventType.equalsIgnoreCase(eventType))
  }

  filterEventsByType(EventTypes.`MOVIECREATEEVENT`).foreach((id, event) => {

    val value = new MovieCreatedEventDeserializer(event).get
    if (value.isDefined) {
      val doc = value.get
      movies.insertOne(document = doc).toFuture().onComplete(_ => println(s"Inserted ${doc}"))
    }

  })

  filterEventsByType(EventTypes.`MOVIEUPDATEEVENT`).

  private val streams = new KafkaStreams(builder.build(), config)
  streams.start()

}

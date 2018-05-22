package com.iamninad

import java.util.Properties

import com.iamninad.event.deserializer.{MovieCreatedEventDeserializer, MovieUpdateEventDeserializer}
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
import org.mongodb.scala.model.{Filters, Updates}
import org.mongodb.scala.{Document, FindObservable, MongoClient, MongoCollection, MongoDatabase}

import scala.util.Success

object EventProcessor extends App {

  import scala.concurrent.ExecutionContext.Implicits.global

  val config = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-12")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", classOf[KafkaAvroSerializer].getName)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.deserializer", classOf[KafkaAvroDeserializer].getName)
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true")
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081/")
    props
  }
  val mongoClient                       = MongoClient("mongodb://localhost:27017")
  val moviedemoDatabase: MongoDatabase  = mongoClient.getDatabase("moviedemo")
  val movies: MongoCollection[Document] = moviedemoDatabase.getCollection("movies")

  private val builder = new StreamsBuilderS()

  def buildEventStream = {
    import AppSerdes.movieBEventSerde.eventConsumed
    builder.stream[Int, BusinessEvent]("events")
  }

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

  filterEventsByType(EventTypes.`MOVIEUPDATEEVENT`).foreach((id, event) => {
    val movie: Option[Document] = new MovieUpdateEventDeserializer(event).get
    val movieDocument           = movies.find(Filters.eq("movie_id", movie.get.getInteger("movie_id")))
    movieDocument.toFuture().map(_.head).onSuccess {
      case data => {
        val document = movie.get.toBsonDocument
        println(s"Relpacing Movie Information ${movie.get.get("movie_id")}")
        document.put("sales", data.get("sales").get)
        movies
          .replaceOne(Filters.eq("_id", data.getObjectId("_id")), document)
          .toFuture()
          .onSuccess {
            case data => println("Movie Information Updated")
          }
      }
    }

  })

  private val streams = new KafkaStreams(builder.build(), config)
  streams.start()

}

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
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.mongodb.scala.{Completed, Document, MongoClient, MongoCollection, MongoDatabase, Observer, SingleObservable}

object EventProcessor extends App {

  import scala.concurrent.ExecutionContext.Implicits.global

  val config = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "helloWorld-18")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", classOf[KafkaAvroSerializer].getName)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.deserializer", classOf[KafkaAvroDeserializer].getName)
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true")
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081/")
    props
  }

  val mongoClient                               = MongoClient("mongodb://localhost:27017")
  private val moviedemoDatabase: MongoDatabase  = mongoClient.getDatabase("moviedemo")
  private val movies: MongoCollection[Document] = moviedemoDatabase.getCollection("movies")

  private val builder = new StreamsBuilderS()

//  builder.stream[Int, BusinessEvent]("events")

  private val streams = new KafkaStreams(builder.build(), config)
//  streams.start()

  val movie: Document =
    Document("movie_id" -> 1, "title" -> "Avenger", "year" -> "2007", "budget" -> 30000, "sales" -> Document("total" -> 100))

//  movies.insertOne(movie).toFuture().onComplete((_) => println("done inserting"))

  movies.find().first().toFuture().onComplete((p) => println(p.get))
  mongoClient.close()
}

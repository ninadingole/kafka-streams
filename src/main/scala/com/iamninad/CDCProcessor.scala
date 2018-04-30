package com.iamninad

import java.util.Properties

import com.iamninad.model.BusinessEvent
import com.lightbend.kafka.scala.streams.{KStreamS, KTableS, StreamsBuilderS}
import dbserver1.moviedemo.movie
import dbserver1.moviedemo.movie.Movie
import dbserver1.moviedemo.moviesales.{Envelope, MovieSales}
import io.confluent.kafka.serializers.{
  AbstractKafkaAvroSerDeConfig,
  KafkaAvroDeserializer,
  KafkaAvroDeserializerConfig,
  KafkaAvroSerializer
}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import scala.collection.JavaConverters._

object CDCProcessor extends App {
  private val TOPIC_PREFIX = "dbserver1.moviedemo."

  val config = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "events-2")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", classOf[KafkaAvroSerializer].getName)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.deserializer", classOf[KafkaAvroDeserializer].getName)
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true")
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081/")
    props
  }
  private val schemaConfig = Map("schema.registry.url" -> "http://localhost:8081", "auto.register.schemas" -> "true").asJava

  private def getTopic(topicName: String): String = {
    TOPIC_PREFIX + topicName
  }

  private val builder = new StreamsBuilderS()

  private def buildMovieStream: KStreamS[String, movie.Envelope] = {
    import AppSerdes.movieSerde.consumed
    builder.stream[String, movie.Envelope](getTopic("movie"))
  }

  private def buildMovieSalesStream = {
    import AppSerdes.movieSalesSerde.consumed
    builder.stream[String, Envelope](getTopic("movie_sales"))
  }

  private def filterSalesStreamForCreations = {
    buildMovieSalesStream
      .filter((id, value) => {
        println("filtering sales creation message")
        value.op.equalsIgnoreCase("c")
      })
  }

  def filterMovieStreamForCreations = {
    buildMovieStream
      .filter((id, value) => {
        println("filtering movie creation messsage")
        value.op.equalsIgnoreCase("C")
      })
  }

  def createMovieBusinessEvent = {
    import AppSerdes.movieBEventSerde.{joined, salesSerialized}
    val movieStream = filterMovieStreamForCreations
    val salesStream = filterSalesStreamForCreations

    val envelopExtractedMovie: KStreamS[Int, Movie] =
      movieStream.map((id, value) => (value.after.get.movie_id.get, value.after.get))
    val envelopeExtractedSale: KTableS[Int, MovieSales] = salesStream
      .map((stringId: String, value) => (value.after.get.movie_id.get, value.after.get))
      .groupByKey
      .reduce((old: MovieSales, newSale: MovieSales) => newSale)

    envelopExtractedMovie.join(envelopeExtractedSale, (movie: Movie, movieSale: MovieSales) => {
      println("Created Business Event")
      val serializer = new KafkaAvroSerializer()
      serializer.configure(schemaConfig, false)
      val movieSerialized = serializer.serialize("events", AppSerdes.movieBEventSerde.movieFormat.to(movie))
      val salesSerialized = serializer.serialize("events", AppSerdes.movieBEventSerde.saleFormat.to(movieSale))

      val map = Map("movie" -> movieSerialized, "sale" -> salesSerialized)
      BusinessEvent("MovieCreatedEvent", map)

    })
  }

  def sendMovieBussinessEventToTopic = {
    import AppSerdes.movieBEventSerde.eventProduced
    createMovieBusinessEvent.to("events")
  }

  sendMovieBussinessEventToTopic

  private val streams = new KafkaStreams(builder.build(), config)
  streams.start()

}

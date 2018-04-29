package com.iamninad

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.iamninad.model.MovieAndSalesBEvent
import com.lightbend.kafka.scala.streams.{KStreamS, KTableS, StreamsBuilderS}
import dbserver1.moviedemo.MOVIE
import dbserver1.moviedemo.MOVIE.Value
import dbserver1.moviedemo.MOVIE_SALES.{Envelope, MovieSales}
import io.confluent.kafka.serializers.{
  AbstractKafkaAvroSerDeConfig,
  KafkaAvroDeserializer,
  KafkaAvroDeserializerConfig,
  KafkaAvroSerializer
}
import org.apache.kafka.streams.kstream.{JoinWindows, Materialized, Printed}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object CDCProcessor extends App {
  private val TOPIC_PREFIX = "dbserver1.moviedemo."

  val config = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "helloWorld-16")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", classOf[KafkaAvroSerializer].getName)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.deserializer", classOf[KafkaAvroDeserializer].getName)
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true")
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081/")
    props
  }

  private def getTopic(topicName: String): String = {
    TOPIC_PREFIX + topicName
  }

  private val builder = new StreamsBuilderS()

  private def buildMovieStream: KStreamS[String, MOVIE.Envelope] = {
    import AppSerdes.movieSerde.consumed
    builder.stream[String, MOVIE.Envelope](getTopic("movie"))
  }

  private def buildMovieSalesStream = {
    import AppSerdes.movieSalesSerde.consumed
    builder.stream[String, Envelope](getTopic("movie_sales"))
  }

  private def filterSalesStreamForCreations = {
    buildMovieSalesStream
      .filter((id, value) => {
        value.op.equalsIgnoreCase("c")
      })
  }

  def filterMovieStreamForCreations = {
    buildMovieStream
      .filter((id, value) => {
        value.op.equalsIgnoreCase("C")
      })
  }

  def createMovieBusinessEvent = {
    import AppSerdes.movieBEventSerde.{joined, salesSerialized}
    val movieStream = filterMovieStreamForCreations
    val salesStream = filterSalesStreamForCreations

    val envelopExtractedMovie: KStreamS[Int, Value] =
      movieStream.map((id, value) => (value.after.get.movie_id.get, value.after.get))
    val envelopeExtractedSale: KTableS[Int, MovieSales] = salesStream
      .map((stringId: String, value) => (value.after.get.movie_id.get, value.after.get))
      .groupByKey
      .reduce((old: MovieSales, newSale: MovieSales) => newSale)

    envelopExtractedMovie.join(envelopeExtractedSale,
                               (movie: Value, movieSale: MovieSales) => MovieAndSalesBEvent(movie, movieSale))
  }

  createMovieBusinessEvent.print(Printed.toSysOut())

  private val streams = new KafkaStreams(builder.build(), config)
  streams.start()

}

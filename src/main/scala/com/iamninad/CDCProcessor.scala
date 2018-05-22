package com.iamninad

import java.util.Properties

import com.iamninad.filter.{MovieCreatedFilter, MovieUpdateFilter}
import com.iamninad.model.BusinessEvent
import com.iamninad.util.{EventTypes, Utils}
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
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import scala.collection.JavaConverters._

object CDCProcessor extends App {

  val config = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "events-16")
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

  private val builder = new StreamsBuilderS()

  private def buildMovieStream: KStreamS[String, movie.Envelope] = {
    import AppSerdes.movieSerde.consumed
    builder.stream[String, movie.Envelope](Utils.getTopic("movie"))
  }

  private def buildMovieSalesStream = {
    import AppSerdes.movieSalesSerde.consumed
    builder.stream[String, Envelope](Utils.getTopic("movie_sales"))
  }

  val movieStream = buildMovieStream
  val saleStream  = buildMovieSalesStream

  private def filterSalesStreamForCreations = {
    saleStream
      .filter((id, value) => {
        println("filtering sales creation message")
        value.op.equalsIgnoreCase("c")
      })
  }

  def createMovieBusinessEvent = {
    import AppSerdes.movieBEventSerde.{joined, salesSerialized}
    val movieFilteredStream = new MovieCreatedFilter().filter(movieStream)
    val salesFilteredStream = filterSalesStreamForCreations

    val envelopExtractedMovie: KStreamS[Int, Movie] =
      movieFilteredStream.map((id, value) => (value.after.get.movie_id.get, value.after.get))
    val envelopeExtractedSale = salesFilteredStream.map((id, value) => (value.after.get.movie_id.get, value.after.get))
//    val envelopeExtractedSale: KTableS[Int, MovieSales] = salesFilteredStream
//      .map((_: String, value) => (value.after.get.movie_id.get, value.after.get))
//      .groupByKey
//      .reduce((_: MovieSales, newSale: MovieSales) => newSale)

    envelopExtractedMovie.join(envelopeExtractedSale, (movie: Movie, movieSale: MovieSales) => {
      println("Created Business Event")
      val serializer = new KafkaAvroSerializer()
      serializer.configure(schemaConfig, false)
      val movieSerialized = serializer.serialize(Utils.getTopic("movie"), AppSerdes.movieBEventSerde.movieFormat.to(movie))
      val salesSerialized =
        serializer.serialize(Utils.getTopic("movie_sales"), AppSerdes.movieBEventSerde.saleFormat.to(movieSale))

      val map = Map("movie" -> movieSerialized, "sale" -> salesSerialized)
      BusinessEvent(EventTypes.`MOVIECREATEEVENT`, map)

    }, JoinWindows.of(3000))
  }

  def emitMovieBussinessEventToTopic = {
    import AppSerdes.movieBEventSerde.eventProduced
    createMovieBusinessEvent.to("events")
  }

  emitMovieBussinessEventToTopic

  def createMovieUpdateEvent = {
    val updateStream = new MovieUpdateFilter().filter(movieStream)
    updateStream.map((id, envelop) => {
      val before = envelop.before.get
      val after  = envelop.after.get

      val serializer = new KafkaAvroSerializer()
      serializer.configure(schemaConfig, false)

      val beforeMovieSerialized = serializer.serialize("events", AppSerdes.movieBEventSerde.movieFormat.to(before))
      val afterMovieSerialized  = serializer.serialize("events", AppSerdes.movieBEventSerde.movieFormat.to(after))
      println("Movie Update Business Event Created")
      (after.movie_id.get,
       BusinessEvent(EventTypes.`MOVIEUPDATEEVENT`, Map("before" -> beforeMovieSerialized, "after" -> afterMovieSerialized)))
    })
  }

  def emitMovieUpdateEvent = {
    import AppSerdes.movieBEventSerde.eventProduced
    createMovieUpdateEvent.to("events")
  }

  emitMovieUpdateEvent

  private val streams = new KafkaStreams(builder.build(), config)
  streams.start()

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      println("Running Shutdown Procedure for kafka streams")
      streams.close()

      streams.cleanUp()
    }
  })

}

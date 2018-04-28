package com.iamninad

import java.util.Properties

import com.iamninad.deserializer.AbstractDeserializer
import com.iamninad.model.Movie
import com.lightbend.kafka.scala.streams.{DefaultSerdes, Deserializer, Serializer, StreamsBuilderS}
import com.sksamuel.avro4s.{FromRecord, RecordFormat}
import dbserver1.moviedemo.MOVIE.Envelope
import dbserver1.moviedemo.MOVIE_SALES.{MovieSales}
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer, KafkaAvroDeserializerConfig, KafkaAvroSerializer}
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter, SpecificRecord}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.streams.kstream.{KStreamBuilder, Printed}
import org.apache.kafka.streams.{Consumed, KafkaStreams, StreamsConfig}

import scala.collection.JavaConverters._

object HelloWorld extends App {

  val config = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "helloWorld-7")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", classOf[KafkaAvroSerializer].getName)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.deserializer", classOf[KafkaAvroDeserializer].getName)
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true")
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,"http://localhost:8081/")
    props
  }


  val serdeConfig = Map("schema.registry.url" -> "http://localhost:8081/",
  "specific.avro.reader" -> "false").asJava

  val cachedSchemaRegistryClient = new CachedSchemaRegistryClient("http://localhost:8081", 100)

//  val se1 = new SpecificAvroSerde[Movie]()
//  se1.configure(serdeConfig, false)
//  val de1 = new SpecificAvroSerde[Movie]()
//  de1.configure(serdeConfig, false)


  val de = new AbstractDeserializer[MovieSales]()

  val se = new Serializer[MovieSales] {
    override def serialize(data: MovieSales): Array[Byte] = {
      val ser = new KafkaAvroSerializer()
      ser.configure(serdeConfig, false)
      val value = RecordFormat[MovieSales]
      val bytes = ser.serialize(null, value.to(data))
      bytes
    }
  }


//  private val client = new CachedSchemaRegistryClient("http://localhost:8081",100)
//  private val schema: Schema = client.getByID(3)
//  println(schema.toString(true))

  private val builder = new StreamsBuilderS()

  implicit val consumed:Consumed[String, MovieSales] = Consumed.`with`(DefaultSerdes.stringSerde, Serdes.serdeFrom(se, de))

  builder.stream[String, MovieSales]("dbserver1.moviedemo.movie_sales").print(Printed.toSysOut())


  private val streams = new KafkaStreams(builder.build(), config)
  streams.start()

}

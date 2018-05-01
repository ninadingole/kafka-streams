package com.iamninad.filter

import com.iamninad.{AppSerdes, BaseKafkaStreamTest, CaseClassSerde}
import com.lightbend.kafka.scala.streams.StreamsBuilderS
import com.sksamuel.avro4s.RecordFormat
import dbserver1.moviedemo.movie.{Envelope, Movie}
import io.debezium.connector.mysql.Source
import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.streams.EmbeddedKafkaStreamsAllInOne
import net.manub.embeddedkafka.{Codecs, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.scalatest.{FunSuite, Matchers}

class MovieCreatedFilterTest extends BaseKafkaStreamTest {
  implicit val config =
    EmbeddedKafkaConfig(kafkaPort = 7000, zooKeeperPort = 7001)

  val (inTopic, outTopic) = ("in", "out")

  test("testFilter") {
    import AppSerdes.movieSerde.{consumed, produced}

    val streamBuilder = new StreamsBuilderS()
    val stream        = streamBuilder.stream[String, Envelope]("in")
    val value         = new MovieCreatedFilter().filter(stream)
    val sourceTest    = new Source(name = "test", server_id = 1L, ts_sec = 1L, file = "test", pos = 1L, row = 0)

    value.to("out")
    runStreams(Seq(inTopic, outTopic), streamBuilder.build()) {
      import TestImplicits._
      val movie     = Movie(Some(101), Some("test"), Some("2008"), Some(20000))
      val movie2    = Movie(Some(102), Some("test2"), Some("2009"), Some(20000))
      val envelope  = Envelope(before = None, after = Some(movie), op = "c", source = sourceTest)
      val envelope2 = Envelope(before = None, after = Some(movie2), op = "u", source = sourceTest)
      publishToKafka("in", envelope)
      publishToKafka("in", envelope2)
      withConsumer[String, Envelope, Unit] { consumer =>
        val consumed: Stream[(String, Envelope)] = consumer.consumeLazily[(String, Envelope)](outTopic)
        consumed.size shouldBe 1
        val tuples = consumed.take(1).toList.head
        tuples._2 shouldBe envelope
      }
    }
  }
  object TestImplicits {
    implicit val recordFormatEnvelop: RecordFormat[Envelope]                            = AppSerdes.movieSerde.format
    val caseclass: CaseClassSerde[Envelope]                                             = AppSerdes.movieSerde.movieEnvelopserde
    implicit val serilizer: Serializer[Envelope]                                        = caseclass.serializer()
    implicit val envelopeDeserializer: Deserializer[Envelope]                           = caseclass.deserializer()
    implicit val consumerRecord: ConsumerRecord[String, Envelope] => (String, Envelope) = cr => (cr.key(), cr.value())
    implicit val deserializer: Deserializer[String]                                     = Codecs.stringDeserializer

  }
}
